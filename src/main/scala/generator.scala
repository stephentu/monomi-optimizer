import scala.collection.mutable.{ ArrayBuffer, HashMap }

trait Generator extends Traversals with Transformers {

  private def topDownTraverseContext(start: Node, ctx: Context)(f: Node => Boolean) = {
    topDownTraversal(start) {
      case e if e.ctx == ctx => f(e)
      case _ => false
    }
  }

  private def topDownTransformContext(start: Node, ctx: Context)(f: Node => (Option[Node], Boolean)) = {
    topDownTransformation(start) {
      case e if e.ctx == ctx => f(e)
      case _ => (None, false)
    }
  }

  private def replaceWith(e: SqlExpr) = (Some(e), false)

  private val keepGoing = (None, true)

  private def negate(f: Node => Boolean): Node => Boolean = (n: Node) => { !f(n) }

  private def resolveAliases(e: SqlExpr): SqlExpr = {
    topDownTransformation(e) {
      case FieldIdent(_, _, ProjectionSymbol(name, ctx), _) =>
        val expr1 = ctx.lookupProjection(name).get
        replaceWith(resolveAliases(expr1))
      case x => keepGoing
    }.asInstanceOf[SqlExpr]
  }

  private def splitTopLevelConjunctions(e: SqlExpr): Seq[SqlExpr] = {
    def split(e: SqlExpr, buffer: ArrayBuffer[SqlExpr]): ArrayBuffer[SqlExpr] = {
      e match {
        case And(lhs, rhs, _) =>
          split(lhs, buffer)
          split(rhs, buffer)
        case _ => buffer += e
      }
      buffer
    }
    split(e, new ArrayBuffer[SqlExpr]).toSeq
  }

  private def splitTopLevelClauses(e: SqlExpr): Seq[SqlExpr] = {
    def split(e: SqlExpr, buffer: ArrayBuffer[SqlExpr]): ArrayBuffer[SqlExpr] = {
      e match {
        case And(lhs, rhs, _) =>
          split(lhs, buffer)
          split(rhs, buffer)
        case Or(lhs, rhs, _) =>
          split(lhs, buffer)
          split(rhs, buffer)
        case _ => buffer += e
      }
      buffer
    }
    split(e, new ArrayBuffer[SqlExpr]).toSeq
  }

  private def foldTopLevelConjunctions(s: Seq[SqlExpr]): SqlExpr = {
    s.reduceLeft( (acc: SqlExpr, elem: SqlExpr) => And(acc, elem) )
  }

  // the return value is:
  // (relation name in e's ctx, global table name, expr out of the global table)
  //
  // NOTE: relation name is kind of misleading. consider the following example:
  //   SELECT ... FROM ( SELECT n1.x + n1.y AS foo FROM n1 WHERE ... ) AS t
  //   ORDER BY foo
  //
  // Suppose we precompute (x + y) for table n1. If we call findOnionableExpr() on
  // "foo", then we'll get (t, n1, x + y) returned. This works under the assumption
  // that we'll project the pre-computed (x + y) from the inner SELECT
  private def findOnionableExpr(e: SqlExpr): Option[(String, String, SqlExpr)] = {
    val ep = resolveAliases(e)
    val r = ep.getPrecomputableRelation
    if (r.isDefined) {
      // canonicalize the expr
      val e0 = topDownTransformation(ep) {
        case FieldIdent(_, name, _, _) => replaceWith(FieldIdent(None, name))
        case x                         => (Some(x.copyWithContext(null)), true)
      }.asInstanceOf[SqlExpr]
      Some((r.get._1, r.get._2, e0))
    } else {
      e match {
        case FieldIdent(_, _, ColumnSymbol(relation, name, ctx), _) =>
          if (ctx.relations(relation).isInstanceOf[SubqueryRelation]) {
            // recurse on the sub-relation
            ctx
              .relations(relation)
              .asInstanceOf[SubqueryRelation]
              .stmt
              .ctx
              .lookupProjection(name)
              .flatMap(findOnionableExpr)
              .map(_.copy(_1 = relation))
          } else None
        case _ => None
      }
    }
  }

  private def encTblName(t: String) = t + "_enc"

  def generatePlanFromOnionSet(stmt: SelectStmt, onionSet: OnionSet): PlanNode =
    generatePlanFromOnionSet0(stmt, onionSet, PreserveOriginal)

  abstract trait EncContext {
    def needsProjections: Boolean = true
  }

  case object PreserveOriginal extends EncContext

  case object PreserveCardinality extends EncContext {
    override def needsProjections = false
  }

  // onions are a mask of acceptable values (all onions must be non zero)
  // if require is true, the plan is guaranteed to return each projection
  // with the given onion. if require is false, the generator uses onions
  // as merely a hint about what onions are preferable
  case class EncProj(onions: Seq[Int], require: Boolean) extends EncContext {
    assert(onions.filter(_ == 0).isEmpty)
  }

  // if encContext is PreserveOriginal, then the plan node generated faithfully
  // recreates the original statement- that is, the result set has the same
  // (unencrypted) type as the result set of stmt.
  //
  // if encContext is PreserveCardinality, then this function is free to generate
  // plans which only preserve the *cardinality* of the original statement. the
  // result set, however, is potentially left encrypted. this is useful for generating
  // plans such as subqueries to EXISTS( ... ) calls
  //
  // if encContext is EncProj, then stmt is expected to have exactly onions.size()
  // projection (this is asserted)- and plan node is written so it returns the
  // projections encrypted with the onion given by the onions sequence
  private def generatePlanFromOnionSet0(
    stmt: SelectStmt, onionSet: OnionSet, encContext: EncContext): PlanNode = {

    //println("generatePlanFromOnionSet0()")
    //println("stmt: " + stmt.sql)
    //println("encContext: " + encContext)

    encContext match {
      case EncProj(o, _) =>
        assert(stmt.ctx.projections.size == o.size)
      case _ =>
    }

    val subRelnGen = new NameGenerator("subrelation")

    // don't call with a literal
    // return value is sequence of hom_descriptors
    def getSupportedHOMExpr(e: SqlExpr, subrels: Map[String, PlanNode]):
      Option[Seq[SqlExpr]] = {
      // TODO: support literal, w/ special "literal hom descriptor"
      assert(!e.isLiteral)
      val e0 = findOnionableExpr(e)
      e0.map { case (r, t, x) =>

        val qual = if (r == t) encTblName(t) else r

        // TODO: not sure if this actually correct
        val name = e match {
          case fi @ FieldIdent(_, _, ColumnSymbol(relation, name0, ctx), _)
            if ctx.relations(relation).isInstanceOf[SubqueryRelation] => name0
          case _ => "rowid"
        }

        val h = onionSet.lookupPackedHOM(t, x).map
        if (h.isEmpty) None else Some((FieldIdent(Some(qual), "rowid"), t, h))

      }.orElse {
        // TODO: this is hacky -
        // special case- if we looking at a field projection
        // from a subquery relation
        e match {
          case fi @ FieldIdent(_, _, ColumnSymbol(relation, name, ctx), _)
            if ctx.relations(relation).isInstanceOf[SubqueryRelation] =>

            val idx =
              ctx
                .relations(relation)
                .asInstanceOf[SubqueryRelation]
                .stmt.ctx.lookupNamedProjectionIndex(name).get

            // TODO: what do we do if the relation tupleDesc is in vector context
            assert(!subrels(relation).tupleDesc(idx)._2)
            val po = subrels(relation).tupleDesc(idx)._1.getOrElse(Onions.PLAIN)
            if ((po & Onions.HOM) != 0) Some(Seq(fi.copyWithContext(null)))
            else None

          case _ => None
        }
      }
    }

    // return a *server-side expr* which is equivalent to e under onion o,
    // if possible. otherwise return None. o can be a bitmask of allowed
    // onions. use the return value to determine which onion was chosen.
    //
    // handles literals properly
    //
    // do not call getSupportedExpr for HOM. use getSupportedHOMExpr() instead
    //
    // postcondition: if ret is not None, (o & ret._2) != 0
    def getSupportedExpr(e: SqlExpr, o: Int, subrels: Map[String, PlanNode]):
      Option[(SqlExpr, Int)] = {
      assert((o & Onions.HOM) == 0)
      if (e.isLiteral) {
        // easy case
        Onions.pickOne(o) match {
          case Onions.PLAIN => Some((e.copyWithContext(null).asInstanceOf[SqlExpr], Onions.PLAIN))
          case o0           => Some((NullLiteral(), o0)) // TODO: encryption
        }
      } else {
        val e0 = findOnionableExpr(e)
        e0.flatMap { case (r, t, x) =>
          onionSet.lookup(t, x).filter(y => (y._2 & o) != 0).map {
            case (basename, o0) =>
              val qual = if (r == t) encTblName(t) else r
              val choice = Onions.pickOne(o0 & o)

              // TODO: not sure if this actually correct
              val name = e match {
                case fi @ FieldIdent(_, _, ColumnSymbol(relation, name0, ctx), _)
                  if ctx.relations(relation).isInstanceOf[SubqueryRelation] => name0
                case _ => basename + "_" + Onions.str(choice)
              }

              ((FieldIdent(Some(qual), name), choice))
          }
        }.orElse {
          // TODO: this is hacky -
          // special case- if we looking at a field projection
          // from a subquery relation
          e match {
            case fi @ FieldIdent(_, _, ColumnSymbol(relation, name, ctx), _)
              if ctx.relations(relation).isInstanceOf[SubqueryRelation] =>

              val idx =
                ctx
                  .relations(relation)
                  .asInstanceOf[SubqueryRelation]
                  .stmt.ctx.lookupNamedProjectionIndex(name).get

              // TODO: what do we do if the relation tupleDesc is in vector context
              assert(!subrels(relation).tupleDesc(idx)._2)
              val po = subrels(relation).tupleDesc(idx)._1.getOrElse(Onions.PLAIN)
              if ((po & o) != 0) Some((fi.copyWithContext(null), po))
              else None

            case _ => None
          }
        }
      }
    }

    // ClientComputations leave the result of the expr un-encrypted
    case class ClientComputation
      (/* a client side expr for evaluation locally. the result of the expr is assumed
        * to be un-encrypted */
       expr: SqlExpr,

       /* additional encrypted projections needed for conjunction. the tuple is as follows:
        *   ( (original) expr from the client expr which will be replaced with proj,
        *     the actual projection to append to the *server side* query,
        *     the encryption onion which is being projected from the server side query,
        *     whether or not the proj is in vector ctx ) */
       projections: Seq[(SqlExpr, SqlProj, Int, Boolean)],

       /* additional subqueries needed for conjunction. the tuple is as follows:
        *   ( (original) SelectStmt from the conjunction which will be replaced with PlanNode,
        *     the PlanNode to execute )
        */
       subqueries: Seq[(Subselect, PlanNode)]
      ) {

      /** Assumes both ClientComputations are conjunctions, and merges them into a single
       * computation */
      def mergeConjunctions(that: ClientComputation): ClientComputation = {
        ClientComputation(
          And(this.expr, that.expr),
          this.projections ++ that.projections,
          this.subqueries ++ that.subqueries)
      }

      // makes the client side expression SQL to go in a LocalTransform node
      def mkSqlExpr(mappings: Map[SqlProj, Int]): SqlExpr = {
        val pmap = projections.map(x => (x._1, x._2)).toMap
        val smap = subqueries.zipWithIndex.map { case ((s, p), i) => (s, (p, i)) }.toMap

        def testExpr(expr0: SqlExpr): Option[SqlExpr] = expr0 match {
          case Exists(s: Subselect, _) => Some(ExistsSubqueryPosition(smap(s)._2))
          case s: Subselect            => Some(SubqueryPosition(smap(s)._2))
          case e                       => pmap.get(e).map { p => TuplePosition(mappings(p)) }
        }

        def mkExpr(expr0: SqlExpr): SqlExpr = {
          topDownTransformation(expr0) {
            case e: SqlExpr => testExpr(e).map(x => replaceWith(x)).getOrElse(keepGoing)
            case _          => keepGoing
          }.asInstanceOf[SqlExpr]
        }

        // TODO: why do we resolve aliases here?
        testExpr(expr).getOrElse(mkExpr(resolveAliases(expr)))
      }
    }

    var cur = stmt // the current statement, as we update it

    val _hiddenNames = new NameGenerator("_hidden")

    val newLocalFilters = new ArrayBuffer[ClientComputation]
    val localFilterPosMaps = new ArrayBuffer[Map[SqlProj, Int]]

    val newLocalGroupBy = new ArrayBuffer[ClientComputation]
    val localGroupByPosMaps = new ArrayBuffer[Map[SqlProj, Int]]

    // left is position in (original) projection to order by,
    // right is client comp
    //
    // NOTE: the position is NOT a position in the final projection list, but a logical
    //       projection position. this assumes no STAR projections
    val newLocalOrderBy = new ArrayBuffer[Either[Int, ClientComputation]]
    val localOrderByPosMaps = new ArrayBuffer[Map[SqlProj, Int]]

    var newLocalLimit: Option[Int] = None

    // these correspond 1 to 1 with the original projections
    val projPosMaps = new ArrayBuffer[Either[(Int, Int), (ClientComputation, Map[SqlProj, Int])]]

    // these correspond 1 to 1 with the new projections in the encrypted
    // re-written query
    val finalProjs = new ArrayBuffer[(SqlProj, Int, Boolean)]

    case class RewriteContext(onions: Seq[Int], aggContext: Boolean) {
      def this(onion: Int, aggContext: Boolean) = this(Seq(onion), aggContext)

      assert(!onions.isEmpty)
      assert(onions.filterNot(BitUtils.onlyOne).isEmpty)

      def inClear: Boolean = testOnion(Onions.PLAIN)
      def testOnion(o: Int): Boolean = !onions.filter { x => (x & o) != 0 }.isEmpty
      def restrict: RewriteContext = copy(onions = Seq(onions.head))

      def restrictTo(o: Int) = new RewriteContext(o, aggContext)
    }

    def rewriteExprForServer(
      expr: SqlExpr, rewriteCtx: RewriteContext, subrels: Map[String, PlanNode]):
      Either[(SqlExpr, Int), (Option[(SqlExpr, Int)], ClientComputation)] = {

      //println("rewriteExprForServer:")
      //println("  expr: " + expr.sql)
      //println("  rewriteCtx: " + rewriteCtx)

      // this is just a placeholder in the tree
      val cannotAnswerExpr = replaceWith(IntLiteral(1))

      def doTransform(e: SqlExpr, curRewriteCtx: RewriteContext):
        Either[(SqlExpr, Int), ClientComputation] = {

        def doTransformServer(e: SqlExpr, curRewriteCtx: RewriteContext):
          Option[(SqlExpr, Int)] = {

          val onionRetVal = new SetOnce[Int] // TODO: FIX THIS HACK
          var _exprValid = true
          def bailOut = {
            _exprValid = false
            cannotAnswerExpr
          }

          val newExpr = topDownTransformation(e) {

            case Or(l, r, _) if curRewriteCtx.inClear =>
              onionRetVal.set(Onions.PLAIN)
              CollectionUtils.optAnd2(
                doTransformServer(l, curRewriteCtx.restrictTo(Onions.PLAIN)),
                doTransformServer(r, curRewriteCtx.restrictTo(Onions.PLAIN))).map {
                  case ((l0, _), (r0, _)) => replaceWith(Or(l0, r0))
                }.getOrElse(bailOut)

            case And(l, r, _) if curRewriteCtx.inClear =>
              onionRetVal.set(Onions.PLAIN)
              CollectionUtils.optAnd2(
                doTransformServer(l, curRewriteCtx.restrictTo(Onions.PLAIN)),
                doTransformServer(r, curRewriteCtx.restrictTo(Onions.PLAIN))).map {
                  case ((l0, _), (r0, _)) => replaceWith(And(l0, r0))
                }.getOrElse(bailOut)

            case eq: EqualityLike if curRewriteCtx.inClear =>
              onionRetVal.set(Onions.PLAIN)

              def handleOneSubselect(ss: Subselect, expr: SqlExpr) = {
                assert(!expr.isInstanceOf[Subselect])
                val onions = Seq(Onions.PLAIN, Onions.DET, Onions.OPE)
                onions.foldLeft(None : Option[SqlExpr]) {
                  case (acc, onion) =>
                    acc.orElse {
                      val e0 = doTransformServer(expr, curRewriteCtx.restrictTo(onion))
                      e0.flatMap { case (expr, _) =>
                        generatePlanFromOnionSet0(
                          ss.subquery, onionSet, EncProj(Seq(onion), true)) match {
                          case RemoteSql(q0, _, _) => Some(eq.copyWithChildren(expr, Subselect(q0)))
                          case _                   => None
                        }
                      }
                    }
                }.map(replaceWith).getOrElse(bailOut)
              }

              (eq.lhs, eq.rhs) match {
                case (ss0 @ Subselect(q0, _), ss1 @ Subselect(q1, _)) =>
                  def mkSeq(o: Int) = {
                    Seq(generatePlanFromOnionSet0(q0, onionSet, EncProj(Seq(o), true)),
                        generatePlanFromOnionSet0(q1, onionSet, EncProj(Seq(o), true)))
                  }
                  val spPLAINs = mkSeq(Onions.PLAIN)
                  val spDETs   = mkSeq(Onions.DET)
                  val spOPEs   = mkSeq(Onions.OPE)

                  (spPLAINs(0), spPLAINs(1)) match {
                    case (RemoteSql(q0p, _, _), RemoteSql(q1p, _, _)) =>
                      replaceWith(eq.copyWithChildren(Subselect(q0p), Subselect(q1p)))
                    case _ =>
                      (spDETs(0), spDETs(1)) match {
                        case (RemoteSql(q0p, _, _), RemoteSql(q1p, _, _)) =>
                          replaceWith(eq.copyWithChildren(Subselect(q0p), Subselect(q1p)))
                        case _ =>
                          (spOPEs(0), spOPEs(1)) match {
                            case (RemoteSql(q0p, _, _), RemoteSql(q1p, _, _)) =>
                              replaceWith(eq.copyWithChildren(Subselect(q0p), Subselect(q1p)))
                            case _ => bailOut
                          }
                      }
                  }
                case (ss @ Subselect(_, _), rhs) => handleOneSubselect(ss, rhs)
                case (lhs, ss @ Subselect(_, _)) => handleOneSubselect(ss, lhs)
                case (lhs, rhs) =>
                  CollectionUtils.optOr3(
                    CollectionUtils.optAnd2(
                      doTransformServer(lhs, curRewriteCtx.restrictTo(Onions.PLAIN)),
                      doTransformServer(rhs, curRewriteCtx.restrictTo(Onions.PLAIN))),
                    CollectionUtils.optAnd2(
                      doTransformServer(lhs, curRewriteCtx.restrictTo(Onions.DET)),
                      doTransformServer(rhs, curRewriteCtx.restrictTo(Onions.DET))),
                    CollectionUtils.optAnd2(
                      doTransformServer(lhs, curRewriteCtx.restrictTo(Onions.OPE)),
                      doTransformServer(rhs, curRewriteCtx.restrictTo(Onions.OPE))))
                  .map {
                    case ((lfi, _), (rfi, _)) =>
                      replaceWith(eq.copyWithChildren(lfi, rfi))
                  }.getOrElse(bailOut)
              }

            // TODO: don't copy so much code from EqualityLike
            case ieq: InequalityLike if curRewriteCtx.inClear =>
              onionRetVal.set(Onions.PLAIN)

              def handleOneSubselect(ss: Subselect, expr: SqlExpr) = {
                assert(!expr.isInstanceOf[Subselect])
                val onions = Seq(Onions.PLAIN, Onions.OPE)
                onions.foldLeft(None : Option[SqlExpr]) {
                  case (acc, onion) =>
                    acc.orElse {
                      val e0 = doTransformServer(expr, curRewriteCtx.restrictTo(onion))
                      e0.flatMap { case (expr, _) =>
                        generatePlanFromOnionSet0(
                          ss.subquery, onionSet, EncProj(Seq(onion), true)) match {
                          case RemoteSql(q0, _, _) => Some(ieq.copyWithChildren(expr, Subselect(q0)))
                          case _                   => None
                        }
                      }
                    }
                }.map(replaceWith).getOrElse(bailOut)
              }

              (ieq.lhs, ieq.rhs) match {
                case (ss0 @ Subselect(q0, _), ss1 @ Subselect(q1, _)) =>
                  def mkSeq(o: Int) = {
                    Seq(generatePlanFromOnionSet0(q0, onionSet, EncProj(Seq(o), true)),
                        generatePlanFromOnionSet0(q1, onionSet, EncProj(Seq(o), true)))
                  }
                  val spPLAINs = mkSeq(Onions.PLAIN)
                  val spOPEs   = mkSeq(Onions.OPE)
                  (spPLAINs(0), spPLAINs(1)) match {
                    case (RemoteSql(q0p, _, _), RemoteSql(q1p, _, _)) =>
                      replaceWith(ieq.copyWithChildren(Subselect(q0p), Subselect(q1p)))
                    case _ =>
                      (spOPEs(0), spOPEs(1)) match {
                        case (RemoteSql(q0p, _, _), RemoteSql(q1p, _, _)) =>
                          replaceWith(ieq.copyWithChildren(Subselect(q0p), Subselect(q1p)))
                        case _ => bailOut
                      }
                  }
                case (ss @ Subselect(_, _), rhs) => handleOneSubselect(ss, rhs)
                case (lhs, ss @ Subselect(_, _)) => handleOneSubselect(ss, lhs)
                case (lhs, rhs) =>
                  CollectionUtils.optOr2(
                    CollectionUtils.optAnd2(
                      doTransformServer(lhs, curRewriteCtx.restrictTo(Onions.PLAIN)),
                      doTransformServer(rhs, curRewriteCtx.restrictTo(Onions.PLAIN))),
                    CollectionUtils.optAnd2(
                      doTransformServer(lhs, curRewriteCtx.restrictTo(Onions.OPE)),
                      doTransformServer(rhs, curRewriteCtx.restrictTo(Onions.OPE))))
                  .map {
                    case ((lfi, _), (rfi, _)) =>
                      replaceWith(ieq.copyWithChildren(lfi, rfi))
                  }.getOrElse(bailOut)
              }

            // TODO: handle subqueries
            case like @ Like(lhs, rhs, _, _) if curRewriteCtx.inClear =>
              onionRetVal.set(Onions.PLAIN)
              CollectionUtils.optAnd2(
                doTransformServer(lhs, curRewriteCtx.restrictTo(Onions.SWP)),
                doTransformServer(rhs, curRewriteCtx.restrictTo(Onions.SWP))).map {
                  case ((l0, _), (r0, _)) => replaceWith(FunctionCall("searchSWP", Seq(l0, r0, NullLiteral())))
                }.getOrElse(bailOut)

            // TODO: handle subqueries
            case in @ In(e, s, n, _) if curRewriteCtx.inClear =>
              onionRetVal.set(Onions.PLAIN)
              def tryOnion(o: Int) = {
                val t = (Seq(e) ++ s).map(x => doTransformServer(x, curRewriteCtx.restrictTo(o)))
                CollectionUtils.optSeq(t).map { s0 =>
                  replaceWith(In(s0.head._1, s0.tail.map(_._1), n))
                }
              }
              tryOnion(Onions.DET).orElse(tryOnion(Onions.OPE)).getOrElse(bailOut)

            case not @ Not(e, _) if curRewriteCtx.inClear =>
              onionRetVal.set(Onions.PLAIN)
              doTransformServer(e, curRewriteCtx.restrictTo(Onions.PLAIN))
                .map { case (e0, _) => replaceWith(Not(e0)) }.getOrElse(bailOut)

            case ex @ Exists(ss, _) if curRewriteCtx.inClear =>
              onionRetVal.set(Onions.PLAIN)
              generatePlanFromOnionSet0(ss.subquery, onionSet, PreserveCardinality) match {
                case RemoteSql(q, _, _) => replaceWith(Exists(Subselect(q)))
                case _                  => bailOut
              }

            case cs @ CountStar(_) if curRewriteCtx.inClear && curRewriteCtx.aggContext =>
              onionRetVal.set(Onions.PLAIN)
              replaceWith(CountStar())

            case cs @ CountExpr(e, d, _) if curRewriteCtx.inClear && curRewriteCtx.aggContext =>
              onionRetVal.set(Onions.PLAIN)
              doTransformServer(e, RewriteContext(Onions.toSeq(Onions.Countable), false))
                .map { case (e0, _) => replaceWith(CountExpr(e0, d)) }.getOrElse(bailOut)

            case m @ Min(f, _) if curRewriteCtx.testOnion(Onions.OPE) && curRewriteCtx.aggContext =>
              onionRetVal.set(Onions.OPE)
              doTransformServer(f, RewriteContext(Seq(Onions.OPE), false))
                .map { case (e0, _) => replaceWith(Min(e0)) }.getOrElse(bailOut)

            case m @ Max(f, _) if curRewriteCtx.testOnion(Onions.OPE) && curRewriteCtx.aggContext =>
              onionRetVal.set(Onions.OPE)
              doTransformServer(f, RewriteContext(Seq(Onions.OPE), false))
                .map { case (e0, _) => replaceWith(Max(e0)) }.getOrElse(bailOut)

            // TODO: we should do something about distinct
            case s @ Sum(f, d, _) if curRewriteCtx.aggContext =>
              def tryPlain = {
                doTransformServer(f, RewriteContext(Seq(Onions.PLAIN), false))
                  .map { case (e0, _) =>
                    onionRetVal.set(Onions.PLAIN)
                    replaceWith(Sum(e0, d))
                  }
              }
              def tryHom = {
                doTransformServer(f, RewriteContext(Seq(Onions.HOM), false))
                  .map { case (e0, _) =>
                    onionRetVal.set(Onions.HOM)
                    replaceWith(AggCall("hom_add", Seq(e0)))
                  }
              }
              if (curRewriteCtx.inClear && curRewriteCtx.testOnion(Onions.HOM)) {
                tryPlain.orElse(tryHom).getOrElse(bailOut)
              } else if (curRewriteCtx.testOnion(Onions.HOM)) {
                tryHom.getOrElse(bailOut)
              } else { bailOut }

            case cw @ CaseWhenExpr(cases, default, _) =>
              //println("found cw: " + cw.sql)
              //println("onions: " + curRewriteCtx.onions)
              def tryWith(o: Int): Option[SqlExpr] = {
                def processCaseExprCase(c: CaseExprCase) = {
                  val CaseExprCase(cond, expr, _) = c
                  doTransformServer(cond, curRewriteCtx.restrictTo(Onions.PLAIN)).flatMap {
                    case (c0, _) =>
                      doTransformServer(expr, curRewriteCtx.restrictTo(o)).map {
                        case (e0, _) => CaseExprCase(c0, e0)
                      }
                  }
                }
                CollectionUtils.optSeq(cases.map(processCaseExprCase)).flatMap { cases0 =>
                  default match {
                    case Some(d) =>
                      doTransformServer(d, curRewriteCtx.restrictTo(o)).map {
                        case (d0, _) =>
                          onionRetVal.set(o)
                          CaseWhenExpr(cases0, Some(d0))
                      }
                    case None =>
                      onionRetVal.set(o)
                      Some(CaseWhenExpr(cases0, None))
                  }
                }
              }

              curRewriteCtx.onions.foldLeft( None : Option[SqlExpr] ) {
                case (acc, onion) => acc.orElse(tryWith(onion))
              }.map(replaceWith).getOrElse(bailOut)

            case e: SqlExpr if e.isLiteral =>
              onionRetVal.set(curRewriteCtx.onions.head)
              curRewriteCtx.onions.head match {
                case Onions.PLAIN => replaceWith(e.copyWithContext(null).asInstanceOf[SqlExpr])
                case _            => replaceWith(NullLiteral()) // TODO : actual encryption
              }

            case e: SqlExpr =>
              curRewriteCtx
                .onions
                .flatMap(o => getSupportedExpr(e, o, subrels))
                .headOption.map { case (expr, onion) =>
                  assert(BitUtils.onlyOne(onion))
                  onionRetVal.set(onion)
                  replaceWith(expr)
                }.getOrElse(bailOut)

            case e => throw new Exception("should only have exprs under expr clause")
          }.asInstanceOf[SqlExpr]

          if (_exprValid) Some(newExpr, onionRetVal.get.get) else None
        }

        doTransformServer(e, curRewriteCtx) match {
          case Some((e0, o)) =>
            // nice, easy case
            Left((e0, o))
          case None =>
            // ugly, messy case- in this case, we have to project enough clauses
            // to compute the entirety of e locally. we can still make optimizations,
            // however, like replacing subexpressions within the expression with
            // more optimized variants

            // take care of all subselects first
            val subselects = new ArrayBuffer[(Subselect, PlanNode)]
            topDownTraversalWithParent(e) {
              case (Some(_: Exists), s @ Subselect(ss, _)) =>
                val p = generatePlanFromOnionSet0(ss, onionSet, PreserveCardinality)
                subselects += ((s, p))
                false
              case (_, s @ Subselect(ss, _)) =>
                val p = generatePlanFromOnionSet0(ss, onionSet, PreserveOriginal)
                subselects += ((s, p))
                false
              case _ => true
            }

            // return value is:
            // ( expr to replace in e -> ( replacement expr, seq( projections needed ) ) )
            def mkOptimizations(e: SqlExpr):
              Map[SqlExpr, (SqlExpr, Seq[(SqlExpr, SqlProj, Int, Boolean)])] = {

              val ret = new HashMap[SqlExpr, (SqlExpr, Seq[(SqlExpr, SqlProj, Int, Boolean)])]

              def handleBinopSpecialCase(op: Binop): Boolean = {
                //println("handleBinopSpecialCase(): op = " + op.sql)
                // TODO: should get the agg context more correctly
                val a = doTransformServer(op.lhs, RewriteContext(Onions.toSeq(Onions.ALL), true))
                val b = doTransformServer(op.rhs, RewriteContext(Onions.toSeq(Onions.ALL), true))

                a match {
                  case Some((aexpr, ao)) =>
                    b match {
                      case Some((bexpr, bo)) =>
                        val id0 = _hiddenNames.uniqueId()
                        val id1 = _hiddenNames.uniqueId()

                        val expr0 = if (aexpr.isLiteral) aexpr else FieldIdent(None, id0)
                        val expr1 = if (bexpr.isLiteral) bexpr else FieldIdent(None, id1)

                        val projs =
                          (if (aexpr.isLiteral) Seq.empty else Seq((expr0, ExprProj(aexpr, None), ao, false))) ++
                          (if (bexpr.isLiteral) Seq.empty else Seq((expr1, ExprProj(bexpr, None), bo, false)))

                        ret += (op -> (op.copyWithChildren(expr0, expr1), projs))

                        false // stop
                      case _ => true
                    }
                  case _ => true
                }
              }

              topDownTraverseContext(e, e.ctx) {
                case avg @ Avg(f, _, _) if curRewriteCtx.aggContext =>
                  doTransformServer(f, RewriteContext(Seq(Onions.HOM), false)).map { case (fi, _) =>
                    val id0 = _hiddenNames.uniqueId()
                    val id1 = _hiddenNames.uniqueId()

                    val expr0 = FieldIdent(None, id0)
                    val expr1 = FieldIdent(None, id1)

                    val projs = Seq(
                      (expr0, ExprProj(AggCall("hom_add", Seq(fi)), None), Onions.HOM, false),
                      (expr1, ExprProj(CountStar(), None), Onions.PLAIN, false))

                    ret += (avg -> (Div(expr0, expr1), projs))

                    false // stop
                  }.getOrElse(true) // keep traversing

                case _: SqlAgg =>
                  false // don't try to optimize exprs within an agg, b/c that
                        // just messes everything up

                case e: SqlExpr if e.isLiteral => false
                  // literals don't need any optimization

                case b: Div   => handleBinopSpecialCase(b)
                case b: Mult  => handleBinopSpecialCase(b)
                case b: Plus  => handleBinopSpecialCase(b)
                case b: Minus => handleBinopSpecialCase(b)

                case _ => true // keep traversing
              }
              ret.toMap
            }

            //val _generator = new NameGenerator("_projection")
            def mkProjections(e: SqlExpr): Seq[(SqlExpr, SqlProj, Int, Boolean)] = {
              val fields = resolveAliases(e).gatherFields
              def translateField(fi: FieldIdent) = {
                CollectionUtils.optOrEither2(
                  getSupportedExpr(fi, Onions.DET, subrels),
                  getSupportedExpr(fi, Onions.OPE, subrels))
                .getOrElse {
                  println("could not find DET/OPE enc for expr: " + fi)
                  println("orig: " + e.sql)
                  println("subrels: " + subrels)
                  throw new Exception("should not happen")
                } match {
                  case Left((e, _))  => (e, Onions.DET)
                  case Right((e, _)) => (e, Onions.OPE)
                }
              }

              fields.map {
                case (f, false) =>
                  val (ft, o) = translateField(f)
                  //(f, ExprProj(ft, Some(_generator.uniqueId())), o, false)
                  (f, ExprProj(ft, None), o, false)
                case (f, true) =>
                  val (ft, o) = translateField(f)
                  //(f, ExprProj(GroupConcat(ft, ","), Some(_generator.uniqueId())), o, true)
                  (f, ExprProj(GroupConcat(ft, ","), None), o, true)
              }
            }

            val opts = mkOptimizations(e)

            val e0ForProj = topDownTransformContext(e, e.ctx) {
              // replace w/ something dumb, so we can gather fields w/o worrying about it
              case e: SqlExpr if opts.contains(e) => replaceWith(IntLiteral(1))
              case _ => keepGoing
            }.asInstanceOf[SqlExpr]

            val e0 = topDownTransformContext(e, e.ctx) {
              case e: SqlExpr if opts.contains(e) => replaceWith(opts(e)._1)
              case _ => keepGoing
            }.asInstanceOf[SqlExpr]

            Right(ClientComputation(
              e0,
              opts.values.flatMap(_._2).toSeq ++ mkProjections(e0ForProj),
              subselects.toSeq))
          }
      }

      val exprs = splitTopLevelConjunctions(expr).map(x => doTransform(x, rewriteCtx))
      assert(!exprs.isEmpty)

      val sexprs = exprs.flatMap {
        case Left((s, o)) => Seq(s)
        case _            => Seq.empty
      }

      val sonions = exprs.flatMap {
        case Left((s, o)) => Seq(o)
        case _            => Seq.empty
      }

      val ccomps = exprs.flatMap {
        case Right(comp) => Seq(comp)
        case _           => Seq.empty
      }

      if (ccomps.isEmpty) {
        assert(!sexprs.isEmpty)
        Left(
          (foldTopLevelConjunctions(sexprs),
           if (sonions.size == 1) sonions.head else Onions.PLAIN) )
      } else {
        var conjunctions: Option[ClientComputation] = None
        def mergeConjunctions(that: ClientComputation) = {
          conjunctions match {
            case Some(thiz) => conjunctions = Some(thiz mergeConjunctions that)
            case None => conjunctions = Some(that)
          }
        }
        ccomps.foreach(mergeConjunctions)
        Right(
          ((if (sexprs.isEmpty) None
            else Some((foldTopLevelConjunctions(sexprs),
                       if (sonions.size == 1) sonions.head else Onions.PLAIN))),
          conjunctions.get))
      }
    }

    // subquery relations
    def findSubqueryRelations(r: SqlRelation): Seq[SubqueryRelationAST] =
      r match {
        case _: TableRelationAST         => Seq.empty
        case e: SubqueryRelationAST      => Seq(e)
        case JoinRelation(l, r, _, _, _) =>
          findSubqueryRelations(l) ++ findSubqueryRelations(r)
      }

    val subqueryRelations =
      cur.relations.map(_.flatMap(findSubqueryRelations)).getOrElse(Seq.empty)

    val subqueryRelationPlans =
      subqueryRelations.map { subq =>

        // build an encryption vector for this subquery
        val encVec = collection.mutable.Seq.fill(subq.subquery.ctx.projections.size)(0)

        def traverseContext(
          start: Node,
          ctx: Context,
          onion: Int,
          selectFn: (SelectStmt) => Unit): Unit = {

          if (start.ctx != ctx) return

          def add(exprs: Seq[(SqlExpr, Int)]): Boolean = {
            // look for references to elements from this subquery
            exprs.foreach {
              case (e, o) if o != Onions.DET =>
                e match {
                  case FieldIdent(_, _, ColumnSymbol(relation, name, ctx), _) =>
                    if (ctx.relations(relation).isInstanceOf[SubqueryRelation]) {
                      val sr = ctx.relations(relation).asInstanceOf[SubqueryRelation]
                      val projExpr = sr.stmt.ctx.lookupProjection(name)
                      assert(projExpr.isDefined)
                      findOnionableExpr(projExpr.get).foreach { case (_, t, x) =>
                        onionSet.lookup(t, x).filter(y => (y._2 & o) != 0).foreach { _ =>
                          val idx = sr.stmt.ctx.lookupNamedProjectionIndex(name)
                          assert(idx.isDefined)
                          encVec( idx.get ) |= o
                        }
                      }
                    }
                  case _ =>
                }
              case _ =>
            }
            true
          }

          def procExprPrimitive(e: SqlExpr, o: Int) = {
            def binopOp(l: SqlExpr, r: SqlExpr) = {
              getPotentialCryptoOpts(l, Onions.ALL).foreach(add)
              getPotentialCryptoOpts(r, Onions.ALL).foreach(add)
            }

            getPotentialCryptoOpts(e, o) match {
              case Some(exprs) => add(exprs)
              case None =>
                e match {
                  case Subselect(ss, _)                 => selectFn(ss)
                  case Exists(Subselect(ss, _), _)      => selectFn(ss)

                  // one-level deep binop optimizations
                  case Plus(l, r, _)                    => binopOp(l, r)
                  case Minus(l, r, _)                   => binopOp(l, r)
                  case Mult(l, r, _)                    => binopOp(l, r)
                  case Div(l, r, _)                     => binopOp(l, r)

                  case _                                =>
                }
            }
          }

          def procExpr(e: SqlExpr, o: Int) = {
            val clauses = splitTopLevelClauses(e)
            assert(!clauses.isEmpty)
            if (clauses.size == 1) {
              procExprPrimitive(clauses.head, o)
            } else {
              clauses.foreach(c => traverseContext(c, ctx, Onions.PLAIN, selectFn))
            }
          }

          start match {
            case e: SqlExpr          => procExpr(e, onion)
            case ExprProj(e, _, _)   => procExpr(e, onion)
            case SqlGroupBy(k, h, _) =>
              k.foreach(e => procExpr(e, onion))
              h.foreach(e => procExpr(e, Onions.PLAIN))
            case SqlOrderBy(k, _)    =>
              k.foreach(e => procExpr(e._1, onion))
            case _                   => /* no-op */
          }
        }

        def buildForSelectStmt(stmt: SelectStmt): Unit = {
          // TODO: handle relations
          val SelectStmt(p, _, f, g, o, _, ctx) = stmt
          p.foreach(e => traverseContext(e, ctx, Onions.ALL,              buildForSelectStmt))
          f.foreach(e => traverseContext(e, ctx, Onions.PLAIN,            buildForSelectStmt))
          g.foreach(e => traverseContext(e, ctx, Onions.Comparable,       buildForSelectStmt))
          o.foreach(e => traverseContext(e, ctx, Onions.IEqualComparable, buildForSelectStmt))
        }

        // TODO: not the most efficient implementation
        buildForSelectStmt(cur)

        (subq.alias,
         generatePlanFromOnionSet0(
           subq.subquery,
           onionSet,
           EncProj(encVec.map(x => if (x != 0) x else Onions.DET).toSeq, true)))
      }.toMap

    //println("subqplans: " + subqueryRelationPlans)

    // TODO: explicit join predicates (ie A JOIN B ON (pred)).
    // TODO: handle {LEFT,RIGHT} OUTER JOINS

    val finalSubqueryRelationPlans = new ArrayBuffer[PlanNode]

    // relations
    cur = cur.relations.map { r =>
      def rewriteSqlRelation(s: SqlRelation): SqlRelation =
        s match {
          case t @ TableRelationAST(name, a, _) => TableRelationAST(encTblName(name), a)
          case j @ JoinRelation(l, r, _, _, _)  =>
            // TODO: join clauses
            j.copy(left  = rewriteSqlRelation(l),
                   right = rewriteSqlRelation(r)).copyWithContext(null)
          case r @ SubqueryRelationAST(_, name, _) =>
            subqueryRelationPlans(name) match {
              case p : RemoteSql =>
                // if remote sql, then keep the subquery as subquery in the server sql,
                // while adding the plan's subquery children to our children directly
                finalSubqueryRelationPlans ++= p.subrelations
                SubqueryRelationAST(p.stmt, name)
              case p =>
                // otherwise, add a RemoteMaterialize node
                val name = subRelnGen.uniqueId()
                finalSubqueryRelationPlans += RemoteMaterialize(name, p)
                TableRelationAST(name, None)
            }
        }
      cur.copy(relations = Some(r.map(rewriteSqlRelation)))
    }.getOrElse(cur)

    // filters
    cur = cur
      .filter
      .map(x => rewriteExprForServer(x, RewriteContext(Seq(Onions.PLAIN), false), subqueryRelationPlans))
      .map {
        case Left((expr, onion)) =>
          assert(onion == Onions.PLAIN)
          cur.copy(filter = Some(expr))

        case Right((optExpr, comp)) =>
          val comp0 =
            if (cur.groupBy.isDefined) {
              // need to group_concat the projections then, because we have a groupBy context
              val ClientComputation(_, p, _) = comp
              comp.copy(projections = p.map {
                case (expr, ExprProj(e, a, _), o, _) =>
                  // TODO: this isn't quite right
                  (expr, ExprProj(GroupConcat(e, ","), a), o, true)
              })
            } else {
              comp
            }
          newLocalFilters += comp0
          optExpr.map { case (expr, onion) =>
            assert(onion == Onions.PLAIN)
            cur.copy(filter = Some(expr)) }.getOrElse(cur.copy(filter = None))
      }.getOrElse(cur)

    // group by
    cur = {
      // need to check if we can answer the having clause
      val newGroupBy =
        cur
          .groupBy
          .map { gb =>
            gb.having.map { x =>
              (rewriteExprForServer(x, RewriteContext(Seq(Onions.PLAIN), true), subqueryRelationPlans) match {
                case Left((expr, onion)) =>
                  assert(onion == Onions.PLAIN)
                  gb.copy(having = Some(expr))
                case Right((optExpr, comp)) =>
                  newLocalGroupBy += comp
                  optExpr.map { case (expr, onion) =>
                    assert(onion  == Onions.PLAIN)
                    gb.copy(having = Some(expr))
                  }.getOrElse(gb.copy(having = None))
              })
            }.getOrElse(gb)
          }

      // now check the keys
      val newGroupBy0 = newGroupBy.map(gb => gb.copy(keys = {
        gb.keys.map(k =>
          CollectionUtils.optOr2(
            getSupportedExpr(k, Onions.DET, subqueryRelationPlans),
            getSupportedExpr(k, Onions.OPE, subqueryRelationPlans))
          .map(_._1).getOrElse {
            println("Non supported expr: " + k)
            println("subq: " + subqueryRelationPlans)
            throw new RuntimeException("TODO: currently cannot support non-field keys")
          })
      }))
      cur.copy(groupBy = newGroupBy0)
    }

    // order by
    cur = {
      def handleUnsupported(o: SqlOrderBy) = {
        def getKeyInfoForExpr(f: SqlExpr) = {
          getSupportedExpr(f, Onions.OPE, subqueryRelationPlans).map { case (e, _) => (f, e, Onions.OPE) }
          .orElse(getSupportedExpr(f, Onions.DET, subqueryRelationPlans).map { case (e, _) => (f, e, Onions.DET) })
        }
        def mkClientCompFromKeyInfo(f: SqlExpr, fi: SqlExpr, o: Int) = {
          ClientComputation(f, Seq((f, ExprProj(fi, None), o, false)), Seq.empty)
        }
        def searchProjIndex(e: SqlExpr): Option[Int] = {
          if (!e.ctx.projections.filter {
                case WildcardProjection => true
                case _ => false }.isEmpty) {
            // for now, if wildcard projection, don't do this optimization
            return None
          }
          e match {
            case FieldIdent(_, _, ProjectionSymbol(name, _), _) =>
              // named projection is easy
              e.ctx.lookupNamedProjectionIndex(name)
            case _ =>
              // actually do a linear search through the projection list
              e.ctx.projections.zipWithIndex.foldLeft(None : Option[Int]) {
                case (acc, (NamedProjection(_, expr, _), idx)) if e == expr =>
                  acc.orElse(Some(idx))
                case (acc, _) => acc
              }
          }
        }
        val aggCtx = !(cur.groupBy.isDefined && !newLocalFilters.isEmpty)
        newLocalOrderBy ++= (
          o.keys.map { case (k, _) =>
            searchProjIndex(k).map(idx => Left(idx)).getOrElse {
              getKeyInfoForExpr(k)
                .map { case (f, fi, o) => Right(mkClientCompFromKeyInfo(f, fi, o)) }
                .getOrElse {
                  // TODO: why do we need to resolveAliases() here only??
                  rewriteExprForServer(
                    resolveAliases(k),
                    RewriteContext(Seq(Onions.OPE), aggCtx),
                    subqueryRelationPlans) match {

                    case Left((expr, onion)) => Right(mkClientCompFromKeyInfo(k, expr, onion))
                    case Right((None, comp)) => Right(comp)
                    case _                   =>
                      // TODO: in this case we prob need to merge the expr as a
                      // projection of the comp instead
                      throw new RuntimeException("TODO: unimpl")
                  }
                }
              }
          }
        )
        None
      }
      val newOrderBy = cur.orderBy.flatMap(o => {
        if (newLocalFilters.isEmpty && newLocalGroupBy.isEmpty) {
          val mapped =
            o.keys.map(f => (getSupportedExpr(f._1, Onions.OPE, subqueryRelationPlans).map(_._1), f._2))
          if (mapped.map(_._1).flatten.size == mapped.size) {
            // can support server side order by
            Some(SqlOrderBy(mapped.map(f => (f._1.get, f._2))))
          } else {
            handleUnsupported(o)
          }
        } else {
          handleUnsupported(o)
        }
      })
      cur.copy(orderBy = newOrderBy)
    }

    // limit
    cur = cur.copy(limit = cur.limit.flatMap(l => {
      if (newLocalFilters.isEmpty &&
          newLocalGroupBy.isEmpty &&
          newLocalOrderBy.isEmpty) {
        Some(l)
      } else {
        newLocalLimit = Some(l)
        None
      }
    }))


    // projections
    cur = {

      val projectionCache = new HashMap[(SqlExpr, Int), (Int, Boolean)]
      def projectionInsert(p: SqlProj, o: Int, v: Boolean): Int = {
        assert(BitUtils.onlyOne(o))
        assert(p.isInstanceOf[ExprProj])
        val ExprProj(e, _, _) = p
        val (i0, v0) =
          projectionCache.get((e.copyWithContext(null).asInstanceOf[SqlExpr], o))
          .getOrElse {
            // doesn't exist, need to insert
            val i = finalProjs.size
            finalProjs += ((p, o, v))

            // insert into cache
            projectionCache += ((e, o) -> (i, v))

            (i, v)
          }
        assert(v == v0)
        i0
      }

      def processClientComputation(comp: ClientComputation): Map[SqlProj, Int] = {
        comp.projections.map { case (_, p, o, v) =>
          assert(BitUtils.onlyOne(o))
          (p, projectionInsert(p, o, v))
        }.toMap
      }

      newLocalFilters.foreach { c =>
        localFilterPosMaps += processClientComputation(c)
      }

      newLocalGroupBy.foreach { c =>
        localGroupByPosMaps += processClientComputation(c)
      }

      newLocalOrderBy.foreach {
        case Left(_)  => localOrderByPosMaps += Map.empty
        case Right(c) => localOrderByPosMaps += processClientComputation(c)
      }

      if (encContext.needsProjections) {
        cur.projections.zipWithIndex.foreach {
          case (ExprProj(e, a, _), idx) =>
            val onions = encContext match {
              case EncProj(o, r) => if (r) Onions.toSeq(o(idx)) else Onions.completeSeqWithPreference(o(idx))
              case _             => Onions.toSeq(Onions.ALL)
            }
            val aggCtx = !(cur.groupBy.isDefined && !newLocalFilters.isEmpty)
            rewriteExprForServer(e, RewriteContext(onions, aggCtx), subqueryRelationPlans) match {
              case Left((expr, onion)) =>
                assert(BitUtils.onlyOne(onion))
                val stmtIdx = projectionInsert(ExprProj(expr, a), onion, false)
                projPosMaps += Left((stmtIdx, onion))

              case Right((optExpr, comp)) =>
                assert(!optExpr.isDefined)
                val m = processClientComputation(comp)
                projPosMaps += Right((comp, m))
            }
          case (StarProj(_), _) => throw new RuntimeException("TODO: implement me")
        }
      }

      cur.copy(projections = finalProjs.map(_._1).toSeq ++ (if (!finalProjs.isEmpty) Seq.empty else Seq(ExprProj(IntLiteral(1), None))))
    }

    def wrapDecryptionNodeSeq(p: PlanNode, m: Seq[Int]): PlanNode = {
      val td = p.tupleDesc
      val s = m.flatMap { pos =>
        if (td(pos)._1.isDefined) Some(pos) else None
      }.toSeq
      if (s.isEmpty) p else LocalDecrypt(s, p)
    }

    def wrapDecryptionNodeMap(p: PlanNode, m: Map[SqlProj, Int]): PlanNode =
      wrapDecryptionNodeSeq(p, m.values.toSeq)

    val tdesc =
      if (finalProjs.isEmpty) Seq((None, false))
      else {
        finalProjs.map { p =>
          assert(BitUtils.onlyOne(p._2))
          if (p._2 == Onions.PLAIN) ((None, p._3)) else ((Some(p._2), p._3))
        }
      }

    // finalProjs is now useless, so clear it
    finalProjs.clear

    // --filters

    val stage1 =
      newLocalFilters
        .zip(localFilterPosMaps)
        .foldLeft( RemoteSql(cur, tdesc, finalSubqueryRelationPlans.toSeq) : PlanNode ) {
          case (acc, (comp, mapping)) =>
            LocalFilter(comp.mkSqlExpr(mapping),
                        wrapDecryptionNodeMap(acc, mapping),
                        comp.subqueries.map(_._2))
        }

    // --group bys

    val stage2 =
      newLocalGroupBy.zip(localGroupByPosMaps).foldLeft( stage1 : PlanNode ) {
        case (acc, (comp, mapping)) =>
          LocalGroupFilter(comp.mkSqlExpr(mapping),
                           wrapDecryptionNodeMap(acc, mapping),
                           comp.subqueries.map(_._2))
      }

    // --projections

    val decryptionVec = (
      projPosMaps.flatMap {
        case Right((_, m)) => Some(m.values)
        case _ => None
      }.flatten ++ {
        projPosMaps.flatMap {
          case Left((p, o)) if o != Onions.PLAIN => Some(p)
          case _ => None
        }
      }
    ).toSet.toSeq.sorted

    val s0 =
      if (decryptionVec.isEmpty) stage2
      else wrapDecryptionNodeSeq(stage2, decryptionVec)

    val projTrfms = projPosMaps.map {
      case Right((comp, mapping)) =>
        assert(comp.subqueries.isEmpty)
        Right(comp.mkSqlExpr(mapping))
      case Left((p, _)) => Left(p)
    }

    var offset = projTrfms.size
    val auxTrfmMSeq = newLocalOrderBy.zip(localOrderByPosMaps).flatMap {
      case (Left(p), _) => Seq.empty
      case (_, m)       =>
        val r = m.values.zipWithIndex.map {
          case (p, idx) => (p, offset + idx)
        }.toSeq
        offset += m.size
        r
    }
    val auxTrfms = auxTrfmMSeq.map { case (k, _) => Left(k) }

    val trfms = projTrfms ++ auxTrfms

    def isPrefixIdentityTransform(trfms: Seq[Either[Int, SqlExpr]]): Boolean = {
      trfms.zipWithIndex.foldLeft(true) {
        case (acc, (Left(p), idx)) => acc && p == idx
        case (acc, (Right(_), _))  => false
      }
    }

    // if trfms describes purely an identity transform, we can omit it
    val stage3 =
      if (trfms.size == s0.tupleDesc.size &&
          isPrefixIdentityTransform(trfms)) s0
      else LocalTransform(trfms, s0)

    // need to update localOrderByPosMaps with new proj info
    val updateIdx = auxTrfmMSeq.toMap

    (0 until localOrderByPosMaps.size).foreach { i =>
      localOrderByPosMaps(i) =
        localOrderByPosMaps(i).map { case (p, i) => (p, updateIdx(i)) }.toMap
    }

    val stage4 = {
      if (!newLocalOrderBy.isEmpty) {
        assert(newLocalOrderBy.size == stmt.orderBy.get.keys.size)
        assert(newLocalOrderBy.size == localOrderByPosMaps.size)

        // do all the local computations to materialize keys

        val decryptionVec =
          newLocalOrderBy.zip(localOrderByPosMaps).flatMap {
            case (Right(ClientComputation(expr, proj, sub)), m) =>
              if (proj.size == 1 && m.size == 1 &&
                  proj.head._3 == Onions.OPE &&
                  proj.head._1 == expr && sub.isEmpty) Seq.empty else m.values
            case _ => Seq.empty
          }

        val orderTrfms =
          newLocalOrderBy.zip(localOrderByPosMaps).flatMap {
            case (Left(p), _)  => None
            case (Right(c), m) => Some(Right(c.mkSqlExpr(m)))
          }

        var offset = projPosMaps.size
        val orderByVec =
          newLocalOrderBy.zip(localOrderByPosMaps).map {
            case (Left(p), _)  => p
            case (Right(_), _) =>
              val r = offset
              offset += 1
              r
          }.zip(stmt.orderBy.get.keys).map { case (l, (_, t)) => (l, t) }

        if (orderTrfms.isEmpty) {
          LocalOrderBy(
            orderByVec,
            if (!decryptionVec.isEmpty) LocalDecrypt(decryptionVec, stage3) else stage3)
        } else {
          val allTrfms = (0 until projPosMaps.size).map { i => Left(i) } ++ orderTrfms
          LocalTransform(
            (0 until projPosMaps.size).map { i => Left(i) },
            LocalOrderBy(
              orderByVec,
              LocalTransform(allTrfms,
                if (!decryptionVec.isEmpty) LocalDecrypt(decryptionVec, stage3) else stage3)))
        }
      } else {
        stage3
      }
    }

    val stage5 =
      newLocalLimit.map(l => LocalLimit(l, stage4)).getOrElse(stage4)

    encContext match {
      case PreserveOriginal | PreserveCardinality => stage5
      case EncProj(o, require) =>
        assert(stage5.tupleDesc.size == o.size)

        // optimization: see if stage5 is one layer covering a usable plan
        val usuablePlan = stage5 match {
          case LocalDecrypt(_, child) =>
            assert(child.tupleDesc.size == o.size)
            if (child.tupleDesc.zip(o).filterNot {
                  case ((Some(x), _), y) => (x & y) != 0
                  case ((None, _),    y) => (Onions.PLAIN & y) != 0
                }.isEmpty) Some(child) else None
          case _ => None
        }

        // special case if stage5 is usuable
        val stage5td = stage5.tupleDesc

        if (usuablePlan.isDefined) {
          usuablePlan.get
        } else if (!require || (stage5td.zip(o).filter {
              case ((Some(x), _), y) => (x & y) != 0
              case _                 => false
            }.size == stage5td.size)) {
          stage5
        } else {

          //println("----")
          //println("stage5: " + stage5.pretty)
          //println("stage5td: " + stage5td)
          //println("o: " + o)

          val dec =
            stage5td.zip(o).zipWithIndex.flatMap {
              case (((Some(x), _), y), _) if (x & y) != 0 => Seq.empty
              case (((Some(x), _), y), i) if (x & y) == 0 => Seq(i)
              case _                                      => Seq.empty
            }

          val enc =
            stage5td.zip(o).zipWithIndex.flatMap {
              case (((Some(x), _), y), _) if (x & y) != 0 => Seq.empty
              case (((Some(x), _), y), i) if (x & y) == 0 => Seq((i, Onions.pickOne(y)))
              case ((_, y), i)                            => Seq((i, Onions.pickOne(y)))
            }

          val first  = if (dec.isEmpty) stage5 else LocalDecrypt(dec, stage5)
          val second = if (enc.isEmpty) first else LocalEncrypt(enc, first)
          second
        }
    }
  }

  // if we want to answer e all on the server with onion constraints given by,
  // return the set of non-literal expressions and the corresponding bitmask of
  // acceptable onions (that is, any of the onions given is sufficient for a
  // server side rewrite), such that pre-computation is minimal
  private def getPotentialCryptoOpts(e: SqlExpr, o: Int):
    Option[Seq[(SqlExpr, Int)]] = {
    getPotentialCryptoOpts0(e, o)
  }

  private def getPotentialCryptoOpts0(e: SqlExpr, constraints: Int):
    Option[Seq[(SqlExpr, Int)]] = {

    def test(o: Int) = (constraints & o) != 0

    def containsNonPlain(o: Int) = (o & ~Onions.PLAIN) != 0

    def pickNonPlain(o: Int) = Onions.pickOne((o & ~Onions.PLAIN))

    // TODO: this is kind of hacky, we shouldn't have to special case this
    def specialCaseExprOpSubselect(expr: SqlExpr, subselectAgg: SqlAgg) = {
      if (!expr.isLiteral) {
        subselectAgg match {
          case Min(expr0, _) =>
            getPotentialCryptoOpts0(expr0, Onions.OPE)
          case Max(expr0, _) =>
            getPotentialCryptoOpts0(expr0, Onions.OPE)
          case _ => None
        }
      } else None
    }
    e match {
      case Or(l, r, _) if test(Onions.PLAIN) =>
        CollectionUtils.optAnd2(
          getPotentialCryptoOpts0(l, Onions.PLAIN),
          getPotentialCryptoOpts0(r, Onions.PLAIN)).map { case (l, r) => l ++ r }

      case And(l, r, _) if test(Onions.PLAIN) =>
        CollectionUtils.optAnd2(
          getPotentialCryptoOpts0(l, Onions.PLAIN),
          getPotentialCryptoOpts0(r, Onions.PLAIN)).map { case (l, r) => l ++ r }

      case eq: EqualityLike if test(Onions.PLAIN) =>
        (eq.lhs, eq.rhs) match {
          case (lhs, Subselect(SelectStmt(Seq(ExprProj(expr: SqlAgg, _, _)), _, _, _, _, _, _), _)) =>
            specialCaseExprOpSubselect(lhs, expr)
          case (Subselect(SelectStmt(Seq(ExprProj(expr: SqlAgg, _, _)), _, _, _, _, _, _), _), rhs) =>
            specialCaseExprOpSubselect(rhs, expr)
          case (lhs, rhs) =>
            CollectionUtils.optAnd2(
              getPotentialCryptoOpts0(lhs, Onions.DET),
              getPotentialCryptoOpts0(rhs, Onions.DET)).map { case (l, r) => l ++ r }
        }

      case ieq: InequalityLike if test(Onions.PLAIN) =>
        (ieq.lhs, ieq.rhs) match {
          case (lhs, Subselect(SelectStmt(Seq(ExprProj(expr: SqlAgg, _, _)), _, _, _, _, _, _), _)) =>
            specialCaseExprOpSubselect(lhs, expr)
          case (Subselect(SelectStmt(Seq(ExprProj(expr: SqlAgg, _, _)), _, _, _, _, _, _), _), rhs) =>
            specialCaseExprOpSubselect(rhs, expr)
          case (lhs, rhs) =>
            CollectionUtils.optAnd2(
              getPotentialCryptoOpts0(lhs, Onions.OPE),
              getPotentialCryptoOpts0(rhs, Onions.OPE)).map { case (l, r) => l ++ r }
        }

      case Like(lhs, rhs, _, _) if test(Onions.PLAIN) =>
        CollectionUtils.optAnd2(
          getPotentialCryptoOpts0(lhs, Onions.SWP),
          getPotentialCryptoOpts0(rhs, Onions.SWP)).map { case (l, r) => l ++ r }

      case Min(expr, _) if test(Onions.OPE) =>
        getPotentialCryptoOpts0(expr, Onions.OPE)

      case Max(expr, _) if test(Onions.OPE) =>
        getPotentialCryptoOpts0(expr, Onions.OPE)

      case s @ Sum(expr, _, _) if test(Onions.HOM) =>
        getPotentialCryptoOpts0(expr, Onions.HOM)

      case Avg(expr, _, _) if test(Onions.HOM) =>
        getPotentialCryptoOpts0(expr, Onions.HOM)

      case CountStar(_) => Some(Seq.empty)

      case CountExpr(expr, _, _) =>
        getPotentialCryptoOpts0(expr, Onions.DET)

      case CaseWhenExpr(cases, default, _) =>
        def procCaseExprCase(c: CaseExprCase, constraints: Int) = {
          CollectionUtils.optAnd2(
            getPotentialCryptoOpts0(c.cond, Onions.ALL),
            getPotentialCryptoOpts0(c.expr, constraints)).map { case (l, r) => l ++ r }
        }
        default.flatMap { d =>
          CollectionUtils.optSeq(
            cases.map(c => procCaseExprCase(c, constraints)) ++
            Seq(getPotentialCryptoOpts0(d, constraints))).map(_.flatten)
        }.orElse {
          CollectionUtils.optSeq(
            cases.map(c => procCaseExprCase(c, constraints))).map(_.flatten)
        }

      case Not(e, _) =>
        getPotentialCryptoOpts0(e, Onions.PLAIN)

      case In(e, s, _, _) =>
        CollectionUtils.optSeq(
          (Seq(e) ++ s).map(x => getPotentialCryptoOpts0(x, Onions.DET))).map(_.flatten)

      case f : FieldIdent if containsNonPlain(constraints) =>
        Some(Seq((f, pickNonPlain(constraints))))

      case e : SqlExpr if e.isLiteral => Some(Seq.empty)

      case e : SqlExpr
        if containsNonPlain(constraints) && e.getPrecomputableRelation.isDefined =>
        Some(Seq((e, pickNonPlain(constraints))))

      case e => None
    }
  }

  def generateCandidatePlans(stmt: SelectStmt): Seq[PlanNode] = {
    val o = generateOnionSets(stmt)
    val perms = CollectionUtils.powerSetMinusEmpty(o)
    // merge all perms, then unique
    val candidates = perms.map(p => OnionSet.merge(p)).toSet.toSeq
    def fillOnionSet(o: OnionSet): OnionSet = {
      o.complete(stmt.ctx.defns)
    }
    candidates.map(fillOnionSet).map(o => generatePlanFromOnionSet(stmt, o)).toSet.toSeq
  }

  def generateOnionSets(stmt: SelectStmt): Seq[OnionSet] = {

    def traverseContext(
      start: Node,
      ctx: Context,
      onion: Int,
      bootstrap: Seq[OnionSet],
      selectFn: (SelectStmt, Seq[OnionSet]) => Seq[OnionSet]): Seq[OnionSet] = {

      var workingSet : Seq[OnionSet] = bootstrap

      def add(exprs: Seq[(SqlExpr, Int)]): Boolean = {
        val e0 = exprs.map { case (e, o) => findOnionableExpr(e).map(e0 => (e0, o)) }.flatten
        if (e0.size == exprs.size) {
          e0.foreach {
            case ((_, t, e), o) =>
              if (o != Onions.HOM) workingSet.foreach(_.add(t, e, o))
              else workingSet.foreach(_.addPackedHOMToLastGroup(t, e))
          }
          true
        } else false
      }

      def procExprPrimitive(e: SqlExpr, o: Int) = {
        def binopOp(l: SqlExpr, r: SqlExpr) = {
          getPotentialCryptoOpts(l, Onions.ALL).foreach(add)
          getPotentialCryptoOpts(r, Onions.ALL).foreach(add)
        }

        getPotentialCryptoOpts(e, o) match {
          case Some(exprs) => add(exprs)
          case None =>
            e match {
              case Subselect(ss, _)                 => workingSet = selectFn(ss, workingSet)
              case Exists(Subselect(ss, _), _)      => workingSet = selectFn(ss, workingSet)

              // one-level deep binop optimizations
              case Plus(l, r, _)                    => binopOp(l, r)
              case Minus(l, r, _)                   => binopOp(l, r)
              case Mult(l, r, _)                    => binopOp(l, r)
              case Div(l, r, _)                     => binopOp(l, r)

              // TODO: more opts?
              case _                                =>
            }
        }
      }

      def procExpr(e: SqlExpr, o: Int) = {
        val clauses = splitTopLevelClauses(e)
        assert(!clauses.isEmpty)
        if (clauses.size == 1) {
          procExprPrimitive(clauses.head, o)
        } else {
          val sets = clauses.flatMap(c => traverseContext(c, ctx, Onions.PLAIN, bootstrap, selectFn))
          workingSet = workingSet.map { ws =>
            sets.foldLeft(ws) { case (acc, elem) => acc.merge(elem) }
          }
        }
      }

      start match {
        case e: SqlExpr          => procExpr(e, onion)
        case ExprProj(e, _, _)   => procExpr(e, onion)
        case SqlGroupBy(k, h, _) =>
          k.foreach(e => procExpr(e, onion))
          h.foreach(e => procExpr(e, Onions.PLAIN))
        case SqlOrderBy(k, _)    =>
          k.foreach(e => procExpr(e._1, onion))
        case _                   => /* no-op */
      }

      workingSet
    }

    def buildForSelectStmt(stmt: SelectStmt, bootstrap: Seq[OnionSet]): Seq[OnionSet] = {
      val SelectStmt(p, r, f, g, o, _, ctx) = stmt
      val s0 = {
        var workingSet = bootstrap.map(_.copy)
        p.foreach { e =>
          workingSet = traverseContext(e, ctx, Onions.ALL, workingSet, buildForSelectStmt)
        }
        workingSet
      }
      def processRelation(r: SqlRelation): Seq[OnionSet] =
        r match {
          case SubqueryRelationAST(subq, _, _) => buildForSelectStmt(subq, bootstrap.map(_.copy))
          case JoinRelation(l, r, _, _, _)     => processRelation(l) ++ processRelation(r)
          case _                               => Seq.empty
        }
      val s1 = r.map(_.flatMap(processRelation))
      val s2 = f.map(e => traverseContext(e, ctx, Onions.PLAIN, bootstrap.map(_.copy), buildForSelectStmt))
      val s3 = g.map(e => traverseContext(e, ctx, Onions.Comparable, bootstrap.map(_.copy), buildForSelectStmt))
      val s4 = o.map(e => traverseContext(e, ctx, Onions.IEqualComparable, bootstrap.map(_.copy), buildForSelectStmt))
      (s0 ++ s1.getOrElse(Seq.empty) ++ s2.getOrElse(Seq.empty) ++
      s3.getOrElse(Seq.empty) ++ s4.getOrElse(Seq.empty)).filterNot(_.isEmpty)
    }

    buildForSelectStmt(stmt, Seq(new OnionSet))
  }
}
