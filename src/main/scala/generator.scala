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

  private def negate(f: Node => Boolean): Node => Boolean = (n: Node) => { !f(n) }

  //private def isSingleAggGroupStmt(stmt: SelectStmt): Boolean = {
  //  if (stmt.groupBy.isDefined) return false
  //  var found = false
  //  stmt.projections.foreach { p =>
  //    topDownTraverseContext(p, stmt.ctx) {
  //      case _: SqlAgg =>
  //        found = true
  //        false
  //      case _ => !found
  //    }
  //  }
  //  found
  //}

  private def resolveAliases(e: SqlExpr): SqlExpr = {
    topDownTransformation(e) {
      case FieldIdent(_, _, ProjectionSymbol(name, ctx), _) =>
        val expr1 = ctx.lookupProjection(name).get
        (Some(resolveAliases(expr1)), false)
      case x => (None, true)
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

  private val subRelnGen = new NameGenerator("subrelation")

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
        case FieldIdent(_, name, _, _) => (Some(FieldIdent(None, name)), false)
        case x => (Some(x.copyWithContext(null)), true)
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

  def encTblName(t: String) = t + "_enc"

  def generatePlanFromOnionSet(stmt: SelectStmt, onionSet: OnionSet): PlanNode =
    generatePlanFromOnionSet0(stmt, onionSet, PreserveOriginal)

  abstract trait EncContext {
    def needsProjections: Boolean = true
  }
  case object PreserveOriginal extends EncContext
  case object PreserveCardinality extends EncContext {
    override def needsProjections = false
  }

  // TODO: we should support "don't care" columns for optimization
  // ie, Seq[Option[Int]]
  case class EncProj(onions: Seq[Int]) extends EncContext

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

    encContext match {
      case EncProj(o) =>
        assert(stmt.ctx.projections.size == o.size)
      case _ =>
    }

    def getSupportedExpr(e: SqlExpr, o: Int): Option[FieldIdent] = {
      val e0 = findOnionableExpr(e)
      e0.flatMap { case (r, t, x) =>
        onionSet.lookup(t, x).filter(y => (y._2 & o) != 0).map {
          case (basename, _) =>
            val qual = if (r == t) encTblName(t) else r

            // TODO: not sure if this actually correct
            val name = e match {
              case fi @ FieldIdent(_, _, ColumnSymbol(relation, name0, ctx), _)
                if ctx.relations(relation).isInstanceOf[SubqueryRelation] => name0
              case _ => basename + "_" + Onions.str(o)
            }

            FieldIdent(Some(qual), name)
        }
      }.orElse {
        // TODO: this is hacky -
        // special case- if we looking at a field projection
        // from a subquery relation, then it is guaranteed to exist in DET form
        // (since we will use a RemoteMaterialize to make it so)
        e match {
          case fi @ FieldIdent(_, _, ColumnSymbol(relation, name, ctx), _)
            if o == Onions.DET &&
               ctx.relations(relation).isInstanceOf[SubqueryRelation] => Some(fi)
          case _ => None
        }
      }
    }

    val keepGoing = (None, true)

    // ClientComputations leave the result of the expr un-encrypted
    case class ClientComputation
      (/* a client side expr for evaluation locally */
       expr: SqlExpr,

       /* additional encrypted projections needed for conjunction. the tuple is as follows:
        *   ( (original) expr from the client expr which will be replaced with proj,
        *     the actual projection to append to the *server side* query,
        *     the encryption onion which is being projected from the server side query,
        *     whether or not the proj is in vector ctx ) */
       projections: Seq[(SqlExpr, SqlProj, Int, Boolean)],

       /* additional subqueries needed for conjunction. the tuple is as follows:
        *   ( (original) SelectStmt from the conjunction which will be replaced with PlanNode,
        *     the PlanNode to execute,
        *     the EncContext the PlanNode was generated with
        *       (tells about the type of tuples coming from PlanNode) )
        */
       subqueries: Seq[(Subselect, PlanNode, EncContext)]
      ) {

      /** Assumes both ClientComputations are conjunctions, and merges them into a single
       * computation */
      def mergeConjunctions(that: ClientComputation): ClientComputation = {
        ClientComputation(
          And(this.expr, that.expr),
          this.projections ++ that.projections,
          this.subqueries ++ that.subqueries)
      }

      def mkSqlExpr(mappings: Map[SqlProj, Int]): SqlExpr = {
        val pmap = projections.map(x => (x._1, x._2)).toMap
        val smap = subqueries.zipWithIndex.map { case ((s, p, _), i) => (s, (p, i)) }.toMap

        def testExpr(expr0: SqlExpr): Option[SqlExpr] = expr0 match {
          case Exists(s: Subselect, _) =>
            Some(ExistsSubqueryPosition(smap(s)._2))
          case s: Subselect => Some(SubqueryPosition(smap(s)._2))
          case e => pmap.get(e).map { p => TuplePosition(mappings(p)) }
        }

        def mkExpr(expr0: SqlExpr): SqlExpr = {
          topDownTransformation(expr0) {
            case e: SqlExpr => testExpr(e).map(x => (Some(x), false)).getOrElse(keepGoing)
            case _          => keepGoing
          }.asInstanceOf[SqlExpr]
        }

        testExpr(expr).getOrElse(mkExpr(resolveAliases(expr)))
      }
    }

    var cur = stmt // the current statement, as we update it


    val _hiddenNames = new NameGenerator("_hidden")

    val newLocalFilters = new ArrayBuffer[ClientComputation]
    val localFilterPosMaps = new ArrayBuffer[Map[SqlProj, Int]]

    val newLocalGroupBy = new ArrayBuffer[ClientComputation]
    val localGroupByPosMaps = new ArrayBuffer[Map[SqlProj, Int]]

    val newLocalOrderBy = new ArrayBuffer[ClientComputation]
    val localOrderByPosMaps = new ArrayBuffer[Map[SqlProj, Int]]

    var newLocalLimit: Option[Int] = None

    // these correspond 1 to 1 with the original projections
    val projPosMaps = new ArrayBuffer[Either[(Int, Int), (ClientComputation, Map[SqlProj, Int])]]

    // these correspond 1 to 1 with the new projections in the encrypted
    // re-written query
    val finalProjs = new ArrayBuffer[(SqlProj, Int, Boolean)]

    def answerWithoutModification(n: Node) = (Some(n.copyWithContext(null)), false)

    abstract trait RewriteContext {
      def aggsValid: Boolean = false
    }
    case object FilterCtx extends RewriteContext
    case object GroupByKeyCtx extends RewriteContext
    case object GroupByHavingCtx extends RewriteContext {
      override def aggsValid = true
    }
    case object OrderByKeyCtx extends RewriteContext
    case class ProjCtx(o: Int = Onions.ALL) extends RewriteContext {
      override def aggsValid = true
    }
    case class ProjUnaggCtx(o: Int = Onions.ALL) extends RewriteContext

    // rewrite expr under rewriteCtx, breaking up the computation of expr into
    // a server side component and an (optional) client side computation. the
    // semantics of this function depend on the rewriteCtx:
    //
    // 1) if rewriteCtx is a boolean context (FilterCtx, GroupByHavingCtx), then
    //    there are two cases:
    //      A) if there *is no* ClientComputation returned, then the returned
    //         server expr evaluates to the equivalent value as expr does,
    //         in a boolean context. Thus, it can be used as a drop in replacement
    //         in the re-written query.
    //      B) if there *is* a ClientComputation returned, then the returned
    //         server expr will select a strict superset of tuples which the
    //         original expr would have selected. the returned expr should still
    //         be included in the re-written query, but by itself it does not
    //         filter out all the necessary tuples as the original predicate.
    //    in both cases, the 2nd parameter returned is 0
    //
    // 2) if rewriteCtx is a projection context (ProjCtx, ProjUnaggCtx), then
    //    there are two cases:
    //      A) if there *is no* ClientComputation returned, then the returned
    //         server side expr is guaranteed to contain the original expr in
    //         an encrypted form. the onion type is indicated by the second
    //         parameter. A best effort is made to return the column in one
    //         of the onions given by the RewriteContext, but this is not
    //         always possible. A 0 for the second param indicates that the
    //         projection is already in decrypted from (for instance, if the
    //         projection was (expr = const))
    //      B) if there *is* a ClientComputation returned, then the returned
    //         server side expr is meaningless, and can thus be omitted from
    //         the re-written query (it should really be Option[SqlExpr]).
    //         also, the 2nd returned parameter is 0. the projected expr
    //         can be obtained by evaluating the ClientComputation as a
    //         LocalTransform. The result of the ClientComputation will leave
    //         the expr in its original, *decrypted* state.
    //
    // 3) if rewriteCtx is a key context (GroupByKeyCtx, OrderByKeyCtx), then
    //    this case is currently un-implemented

    def rewriteExprForServer(expr: SqlExpr, rewriteCtx: RewriteContext):
      (SqlExpr, Int, Option[ClientComputation]) = {

      // this is ok, b/c we remove units of conjunctions
      val cannotAnswerExpr = (Some(IntLiteral(1)), false)

      // TODO: we need to really enforce curRewriteCtx (it's done very
      // ad-hoc now)
      def doTransform(e: SqlExpr, curRewriteCtx: RewriteContext):
        (SqlExpr, Int, Option[ClientComputation]) = {

        def mkOnionRetVal(o: Int) = curRewriteCtx match {
          case FilterCtx | GroupByHavingCtx => 0
          case ProjCtx(_) | ProjUnaggCtx(_) => o
          case _ => throw new RuntimeException("TODO: impl")
        }

        val onionRetVal = new SetOnce[Int] // TODO: FIX THIS HACK
        var _exprValid = true
        def bailOut = {
          _exprValid = false
          cannotAnswerExpr
        }
        val newExpr = topDownTransformation(e) {
          case Or(_, _, _) =>
            onionRetVal.set(mkOnionRetVal(0))
            keepGoing
          case And(_, _, _) =>
            onionRetVal.set(mkOnionRetVal(0))
            keepGoing

          case eq: EqualityLike =>
            onionRetVal.set(mkOnionRetVal(0))
            def handleOneLiteral(lhs: SqlExpr, rhs: SqlExpr) = {
              assert(!lhs.isLiteral)
              assert(rhs.isLiteral)
              lhs match {
                case ss @ Subselect(q, _) =>
                  val subPlans =
                    Seq(generatePlanFromOnionSet0(q, onionSet, EncProj(Seq(Onions.DET))),
                        generatePlanFromOnionSet0(q, onionSet, EncProj(Seq(Onions.OPE))))
                  val sp0 = subPlans.map(x => if (x.isInstanceOf[RemoteSql]) Some(x) else None)
                  CollectionUtils.optOr2(sp0(0), sp0(1)).map {
                    case RemoteSql(qp, _, _) =>
                      (Some(eq.copyWithChildren(Subselect(qp), NullLiteral())), false)
                  }.getOrElse(bailOut)
                case _ =>
                  CollectionUtils.optOr2(
                    getSupportedExpr(lhs, Onions.DET),
                    getSupportedExpr(lhs, Onions.OPE))
                  .map(fi => (Some(eq.copyWithChildren(fi, NullLiteral())), false))
                  .getOrElse(bailOut)
              }
            }
            def handleNoLiteral(lhs: SqlExpr, rhs: SqlExpr) = {
              assert(!lhs.isLiteral)
              assert(!rhs.isLiteral)

              def handleOne(ss: Subselect, expr: SqlExpr) = {
                assert(!expr.isInstanceOf[Subselect])

                val e0 = getSupportedExpr(expr, Onions.DET)
                val e1 = getSupportedExpr(expr, Onions.OPE)

                val p0 =
                  e0.map(_ =>
                    generatePlanFromOnionSet0(ss.subquery, onionSet, EncProj(Seq(Onions.DET))))

                val p1 =
                  e1.map(_ =>
                    generatePlanFromOnionSet0(ss.subquery, onionSet, EncProj(Seq(Onions.OPE))))

                p0 match {
                  case Some(RemoteSql(q0, _, _)) =>
                    (Some(eq.copyWithChildren(e0.get, Subselect(q0))), false)
                  case _ =>
                    p1 match {
                      case Some(RemoteSql(q1, _, _)) =>
                        (Some(eq.copyWithChildren(e1.get, Subselect(q1))), false)
                      case _ => bailOut
                    }
                }
              }

              (lhs, rhs) match {
                case (ss0 @ Subselect(q0, _), ss1 @ Subselect(q1, _)) =>
                  // try both DETs or both OPEs
                  val spDETs =
                    Seq(generatePlanFromOnionSet0(q0, onionSet, EncProj(Seq(Onions.DET))),
                        generatePlanFromOnionSet0(q1, onionSet, EncProj(Seq(Onions.DET))))
                  val spOPEs =
                    Seq(generatePlanFromOnionSet0(q0, onionSet, EncProj(Seq(Onions.OPE))),
                        generatePlanFromOnionSet0(q1, onionSet, EncProj(Seq(Onions.OPE))))

                  // try spDETs first, then spOPEs
                  (spDETs(0), spDETs(1)) match {
                    case (RemoteSql(q0p, _, _), RemoteSql(q1p, _, _)) =>
                      (Some(eq.copyWithChildren(Subselect(q0p), Subselect(q1p))), false)
                    case _ =>
                      (spOPEs(0), spOPEs(1)) match {
                        case (RemoteSql(q0p, _, _), RemoteSql(q1p, _, _)) =>
                          (Some(eq.copyWithChildren(Subselect(q0p), Subselect(q1p))), false)
                        case _ => bailOut
                      }
                  }
                case (ss @ Subselect(_, _), _) => handleOne(ss, rhs)
                case (_, ss @ Subselect(_, _)) => handleOne(ss, lhs)
                case _ =>
                  CollectionUtils.optOr2(
                    CollectionUtils.optAnd2(
                      getSupportedExpr(lhs, Onions.DET),
                      getSupportedExpr(rhs, Onions.DET)),
                    CollectionUtils.optAnd2(
                      getSupportedExpr(lhs, Onions.OPE),
                      getSupportedExpr(rhs, Onions.OPE)))
                  .map {
                    case (lfi, rfi) =>
                      (Some(eq.copyWithChildren(lfi, rfi)), false)
                  }.getOrElse(bailOut)
              }
            }

            (eq.lhs, eq.rhs) match {
              case (lhs, rhs) if rhs.isLiteral => handleOneLiteral(lhs, rhs)
              case (lhs, rhs) if lhs.isLiteral => handleOneLiteral(rhs, lhs)
              case (lhs, rhs) => handleNoLiteral(lhs, rhs)
            }

          // TODO: handle subqueries
          case ieq: InequalityLike =>
            onionRetVal.set(mkOnionRetVal(0))
            (ieq.lhs, ieq.rhs) match {
              case (lhs, rhs) if rhs.isLiteral =>
                getSupportedExpr(lhs, Onions.OPE).map(fi => (Some(ieq.copyWithChildren(fi, NullLiteral())), false)).getOrElse(bailOut)
              case (lhs, rhs) if lhs.isLiteral =>
                getSupportedExpr(rhs, Onions.OPE).map(fi => (Some(ieq.copyWithChildren(NullLiteral(), fi)), false)).getOrElse(bailOut)
              case (lhs, rhs) =>
                CollectionUtils.optAnd2(
                  getSupportedExpr(lhs, Onions.OPE),
                  getSupportedExpr(rhs, Onions.OPE))
                .map {
                  case (lfi, rfi) =>
                    (Some(ieq.copyWithChildren(lfi, rfi)), false)
                }.getOrElse(bailOut)
            }

          // TODO: handle subqueries
          case like @ Like(lhs, rhs, n, _) =>
            onionRetVal.set(mkOnionRetVal(0))
            (lhs, rhs) match {
              case (lhs, rhs) if rhs.isLiteral =>
                getSupportedExpr(lhs, Onions.SWP).map(fi => (Some(FunctionCall("searchSWP", Seq(fi, NullLiteral(), NullLiteral()))), false)).getOrElse(bailOut)
              case (lhs, rhs) if lhs.isLiteral =>
                getSupportedExpr(rhs, Onions.SWP).map(fi => (Some(FunctionCall("searchSWP", Seq(fi, NullLiteral(), NullLiteral()))), false)).getOrElse(bailOut)
              case (lhs, rhs) =>
                // TODO: can we handle this?
                bailOut
            }

          case in @ In(e, s, _, _) =>
            onionRetVal.set(mkOnionRetVal(0))

            def tryOnion(o: Int) = {
              val t =
                Seq(getSupportedExpr(e, o)) ++
                s.map(x => if (x.isLiteral) Some(NullLiteral()) else getSupportedExpr(x, o))
              CollectionUtils.optSeq(t).map { s0 =>
                (Some(in.copy(elem = s0.head, set = s0.tail)), false)
              }
            }

            tryOnion(Onions.DET).orElse(tryOnion(Onions.OPE)).getOrElse(bailOut)

          case ex @ Exists(ss, _) =>
            val plan = generatePlanFromOnionSet0(ss.subquery, onionSet, PreserveCardinality)
            plan match {
              case RemoteSql(q, _, _) =>
                (Some(ex.copy(select = Subselect(q))), false)
              case _ => bailOut
            }

          case cs @ CountStar(_) if curRewriteCtx.aggsValid =>
            onionRetVal.set(mkOnionRetVal(0))
            answerWithoutModification(cs)

          case m @ Min(f, _) if curRewriteCtx.aggsValid =>
            onionRetVal.set(mkOnionRetVal(Onions.OPE))
            getSupportedExpr(f, Onions.OPE).map(fi => (Some(Min(fi)), false)).getOrElse(bailOut)

          case m @ Max(f, _) if curRewriteCtx.aggsValid =>
            onionRetVal.set(mkOnionRetVal(Onions.OPE))
            getSupportedExpr(f, Onions.OPE).map(fi => (Some(Max(fi)), false)).getOrElse(bailOut)

          case s @ Sum(f, _, _) if curRewriteCtx.aggsValid =>
            onionRetVal.set(mkOnionRetVal(Onions.HOM))
            doTransform(f, ProjCtx(Onions.HOM)) match {
              case (f0, Onions.HOM, None) => (Some(AggCall("hom_add", Seq(f0))), false)
              case _                      => bailOut
            }

          case CaseWhenExpr(cases, default, _) =>

            // TODO: this is a temp hack for now
            val innerCtx = curRewriteCtx match {
              case c : ProjCtx => c
              case _ => ProjCtx(Onions.DET)
            }

            onionRetVal.set(innerCtx.o)

            def processCaseExprCase(c: CaseExprCase) = {
              val CaseExprCase(cond, expr, _) = c
              doTransform(cond, ProjCtx(Onions.DET)) match {
                case (c0, _, None) =>
                  doTransform(expr, innerCtx) match {
                    case (e0, _, None) => Some(CaseExprCase(c0, e0))
                    case _ => None
                  }
                case _ => None
              }
            }

            CollectionUtils.optSeq(cases.map(processCaseExprCase)).map { cases0 =>
              default match {
                case Some(d) =>
                  (doTransform(d, innerCtx) match {
                    case (e0, _, None) => Some(e0)
                    case _ => None
                  }).map { d0 =>
                    (Some(CaseWhenExpr(cases0, Some(d0))), false)
                  }.getOrElse(bailOut)
                case None =>
                  (Some(CaseWhenExpr(cases0, None)), false)
              }
            }.getOrElse(bailOut)

          case e: SqlExpr =>

            def handleProj(onion: Int) = {

              val d = if (e.isLiteral) Some(NullLiteral()) else getSupportedExpr(e, Onions.DET)
              val o = if (e.isLiteral) Some(NullLiteral()) else getSupportedExpr(e, Onions.OPE)
              val h = if (e.isLiteral) Some(NullLiteral()) else getSupportedExpr(e, Onions.HOM)
              val s = if (e.isLiteral) Some(NullLiteral()) else getSupportedExpr(e, Onions.SWP)

              // try to honor the given onions first
              if ((onion & Onions.DET) != 0 && d.isDefined) {
                onionRetVal.set(mkOnionRetVal(Onions.DET))
                (Some(d.get), false)
              } else if ((onion & Onions.OPE) != 0 && o.isDefined) {
                onionRetVal.set(mkOnionRetVal(Onions.OPE))
                (Some(o.get), false)
              } else if ((onion & Onions.HOM) != 0 && h.isDefined) {
                onionRetVal.set(mkOnionRetVal(Onions.HOM))
                (Some(h.get), false)
              } else if ((onion & Onions.SWP) != 0 && s.isDefined) {
                onionRetVal.set(mkOnionRetVal(Onions.HOM))
                (Some(s.get), false)
              } else if (d.isDefined) {
                onionRetVal.set(mkOnionRetVal(Onions.DET))
                (Some(d.get), false)
              } else if (o.isDefined) {
                onionRetVal.set(mkOnionRetVal(Onions.OPE))
                (Some(o.get), false)
              } else if (h.isDefined) {
                onionRetVal.set(mkOnionRetVal(Onions.HOM))
                (Some(h.get), false)
              } else if (s.isDefined) {
                onionRetVal.set(mkOnionRetVal(Onions.SWP))
                (Some(s.get), false)
              } else {
                bailOut
              }
            }

            curRewriteCtx match {
              case FilterCtx | GroupByHavingCtx =>
                println("cannot handle expr: " + e.sql)
                throw new RuntimeException("TODO: need to evaluate in boolean context")
              case ProjCtx(o) => handleProj(o)
              case ProjUnaggCtx(o) => handleProj(o)
              case _ => throw new RuntimeException("TODO: impl")
            }

          case e => throw new Exception("should only have exprs under expr clause")
        }.asInstanceOf[SqlExpr]

        if (_exprValid) {
          // nice, easy case
          (newExpr, onionRetVal.get.getOrElse(0), None)
        } else {
          // ugly, messy case- in this case, we have to project enough clauses
          // to compute the entirety of e locally. we can still make optimizations,
          // however, like replacing subexpressions within the expression with
          // more optimized variants

          // take care of all subselects
          val subselects = new ArrayBuffer[(Subselect, PlanNode, EncContext)]
          topDownTraversalWithParent(e) {
            case (Some(_: Exists), s @ Subselect(ss, _)) =>
              val p = generatePlanFromOnionSet0(ss, onionSet, PreserveCardinality)
              subselects += ((s, p, PreserveCardinality))
              false
            case (_, s @ Subselect(ss, _)) =>
              val p = generatePlanFromOnionSet0(ss, onionSet, PreserveOriginal)
              subselects += ((s, p, PreserveOriginal))
              false
            case _ => true
          }

          // return value is:
          // ( expr to replace in e -> ( replacement expr, seq( projections needed ) ) )
          def mkOptimizations(e: SqlExpr): Map[SqlExpr, (SqlExpr, Seq[(SqlExpr, SqlProj, Int, Boolean)])] = {

            val ret = new HashMap[SqlExpr, (SqlExpr, Seq[(SqlExpr, SqlProj, Int, Boolean)])]

            def handleBinopSpecialCase(op: Binop): Boolean = {
              val a = doTransform(op.lhs, ProjCtx(Onions.ALL))
              val b = doTransform(op.rhs, ProjCtx(Onions.ALL))

              a match {
                case (aexpr, ao, None) =>
                  b match {
                    case (bexpr, bo, None) =>
                      val id0 = _hiddenNames.uniqueId()
                      val id1 = _hiddenNames.uniqueId()

                      val expr0 = FieldIdent(None, id0)
                      val expr1 = FieldIdent(None, id1)

                      val projs = Seq(
                        (expr0, ExprProj(aexpr, None), ao, false),
                        (expr1, ExprProj(bexpr, None), bo, false))

                      ret += (op -> (op.copyWithChildren(expr0, expr1), projs))

                      false // stop
                    case _ => true
                  }
                case _ => true
              }
            }

            topDownTraverseContext(e, e.ctx) {
              case avg @ Avg(f, _, _) if curRewriteCtx.aggsValid =>
                getSupportedExpr(f, Onions.HOM).map(fi => {
                  val id0 = _hiddenNames.uniqueId()
                  val id1 = _hiddenNames.uniqueId()

                  val expr0 = FieldIdent(None, id0)
                  val expr1 = FieldIdent(None, id1)

                  val projs = Seq(
                    (expr0, ExprProj(AggCall("hom_add", Seq(fi)), None), Onions.HOM, false),
                    (expr1, ExprProj(CountStar(), None), 0, false))

                  ret += (avg -> (Div(expr0, expr1), projs))

                  false // stop
                }).getOrElse(true) // keep traversing

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
                getSupportedExpr(fi, Onions.DET),
                getSupportedExpr(fi, Onions.OPE))
              .getOrElse(throw new Exception("should not happen")) match {
                case Left(e) => (e, Onions.DET)
                case Right(e) => (e, Onions.OPE)
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
            case e: SqlExpr if opts.contains(e) => (Some(IntLiteral(1)), false)
            case _ => (None, true)
          }.asInstanceOf[SqlExpr]

          val e0 = topDownTransformContext(e, e.ctx) {
            case e: SqlExpr if opts.contains(e) => (Some(opts(e)._1), false)
            case _ => (None, true)

          }.asInstanceOf[SqlExpr]

          (cannotAnswerExpr._1.get,
           0,
           Some(ClientComputation(
             e0,
             opts.values.flatMap(_._2).toSeq ++ mkProjections(e0ForProj),
             subselects.toSeq)))
        }
      }

      var conjunctions: Option[ClientComputation] = None

      def mergeConjunctions(that: ClientComputation) = {
        conjunctions match {
          case Some(thiz) => conjunctions = Some(thiz mergeConjunctions that)
          case None => conjunctions = Some(that)
        }
      }

      val exprs = splitTopLevelConjunctions(expr).map(x => doTransform(x, rewriteCtx))
      assert(!exprs.isEmpty)

      val serverExpr = foldTopLevelConjunctions(exprs.map(_._1))
      val serverOnion = if (exprs.size == 1) exprs.head._2 else 0
      exprs.map(_._3).foreach(_.foreach(mergeConjunctions))

      (serverExpr, serverOnion, conjunctions)
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
          selectFn: (SelectStmt) => Unit): Unit = {

          def add(exprs: Seq[(SqlExpr, Int)]): Boolean = {
            val e0 = exprs.map(e => getSupportedExpr(e._1, e._2)).flatten
            if (e0.size == exprs.size) {
              // look for references to elements from this subquery
              exprs.foreach { e =>
                e match {
                  case (FieldIdent(_, _, ColumnSymbol(relation, name, ctx), _), o) =>
                    if (ctx.relations(relation).isInstanceOf[SubqueryRelation]) {
                      val idx =
                        ctx
                          .relations(relation)
                          .asInstanceOf[SubqueryRelation]
                          .stmt.ctx.lookupNamedProjectionIndex(name)
                      assert(idx.isDefined)
                      encVec( idx.get ) |= o
                    }
                  case _ =>
                }
              }
              true
            } else false
          }

          def procNode(n: Node) = {
            getPotentialCryptoOpts(n) match {
              case Some(exprs) => add(exprs)
              case None =>
                n match {
                  case Subselect(ss, _) =>
                    selectFn(ss)
                    true
                  case _ => false
                }
            }
          }

          topDownTraverseContext(start, ctx)(negate {
            case e: SqlExpr =>
              val clauses = splitTopLevelClauses(e)
              assert(!clauses.isEmpty)
              if (clauses.size == 1) {
                procNode(clauses.head)
              } else {
                clauses.foreach(c => traverseContext(c, ctx, selectFn))
                true
              }
            case e => procNode(e)
          })
        }

        def buildForSelectStmt(stmt: SelectStmt): Unit = {
          val SelectStmt(p, _, f, g, o, _, ctx) = stmt
          p.foreach(e => traverseContext(e, ctx, buildForSelectStmt))
          f.foreach(e => traverseContext(e, ctx, buildForSelectStmt))
          g.foreach(e => traverseContext(e, ctx, buildForSelectStmt))
          o.foreach(e => traverseContext(e, ctx, buildForSelectStmt))
        }

        // TODO: not the most efficient implementation
        buildForSelectStmt(cur)

        // TODO: we should be more sensitive to what encs the subrelation
        // supports
        (subq,
         generatePlanFromOnionSet0(
           subq.subquery,
           onionSet,
           EncProj(encVec.map(x => if (x != 0) x else Onions.DET).toSeq)))

      }.toMap

    // TODO: explicit join predicates (ie A JOIN B ON (pred)).
    // TODO: handle {LEFT,RIGHT} OUTER JOINS

    val finalSubqueryRelationPlans = new ArrayBuffer[PlanNode]

    // relations
    cur = cur.relations.map { r =>
      def rewriteSqlRelation(s: SqlRelation): SqlRelation =
        s match {
          case t @ TableRelationAST(name, _, _) => t.copy(name = encTblName(name))
          case j @ JoinRelation(l, r, _, _, _)  =>
            j.copy(left  = rewriteSqlRelation(l),
                   right = rewriteSqlRelation(r))
          case r : SubqueryRelationAST =>
            subqueryRelationPlans(r) match {
              case p : RemoteSql =>
                // if remote sql, then keep the subquery as subquery in the server sql,
                // while adding the plan's subquery children to our children directly
                finalSubqueryRelationPlans ++= p.subrelations
                r.copy(subquery = p.stmt)
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
    cur = cur.filter.map(x => rewriteExprForServer(x, FilterCtx)).map {
      case (stmt, _, comps) =>
        val comps0 =
          if (cur.groupBy.isDefined) {
            // need to group_concat the projections then, because we have a groupBy context
            comps.map {
              case cc @ ClientComputation(_, p, _) =>
                cc.copy(projections = p.map {
                  case (expr, ExprProj(e, a, _), o, _) =>
                    // TODO: this isn't quite right
                    (expr, ExprProj(GroupConcat(e, ","), a), o, true)
                })
            }
          } else {
            comps
          }
        comps0.foreach(c => newLocalFilters += c)
        cur.copy(filter = Some(stmt))
    }.getOrElse(cur)

    // group by
    cur = {
      // need to check if we can answer the having clause
      val newGroupBy =
        cur.groupBy.map(gb => gb.having.map(x => rewriteExprForServer(x, GroupByHavingCtx)).map {
          case (stmt, _, comps) =>
            comps.foreach(c => newLocalGroupBy += c)
            gb.copy(having = Some(stmt))
        }.getOrElse(gb))

      // now check the keys
      val newGroupBy0 = newGroupBy.map(gb => gb.copy(keys = {
        gb.keys.map(k =>
          CollectionUtils.optOr2(
            getSupportedExpr(k, Onions.DET),
            getSupportedExpr(k, Onions.OPE))
          .getOrElse {
            println("Non supported expr: " + k)
            throw new RuntimeException("TODO: currently cannot support non-field keys")
          })
      }))
      cur.copy(groupBy = newGroupBy0)
    }

    // order by
    cur = {
      def handleUnsupported(o: SqlOrderBy) = {
        def getKeyInfoForExpr(f: SqlExpr) = {
          getSupportedExpr(f, Onions.OPE).map(e => (f, e, Onions.OPE))
          .orElse(getSupportedExpr(f, Onions.DET).map(e => (f, e, Onions.DET)))
            .orElse(getSupportedExpr(f, Onions.HOM).map(e => (f, e, Onions.HOM)))
        }
        def mkClientCompFromKeyInfo(f: SqlExpr, fi: SqlExpr, o: Int) = {
          ClientComputation(f, Seq((f, ExprProj(fi, None), o, false)), Seq.empty)
        }
        val keyInfo = o.keys.map { case (f, _) => getKeyInfoForExpr(f) }
        if (keyInfo.flatten.size == keyInfo.size) {
          // easy unsupported case- all the keys have some
          // expr form which we can work with as a single field- thus, we don't
          // need to call rewriteExprForServer(), since we want to optimize to
          // prefer OPE over DET (rewriteExprForServer() prefers DET over OPE)
          newLocalOrderBy ++= (
            keyInfo.flatten.map { case (f, fi, o) => mkClientCompFromKeyInfo(f, fi, o) })
        } else {
          // more general case-
          val ctx =
            if (cur.groupBy.isDefined && !newLocalFilters.isEmpty) {
              ProjUnaggCtx()
            } else {
              ProjCtx()
            }
          newLocalOrderBy ++= (
            o.keys.map { case (k, _) =>
              getKeyInfoForExpr(k)
                .map { case (f, fi, o) => mkClientCompFromKeyInfo(f, fi, o) }
                .getOrElse {
                  rewriteExprForServer(resolveAliases(k), ctx) match {
                    case (_, _, Some(comp)) => comp
                    case (fi, o, None) => mkClientCompFromKeyInfo(k, fi, o)
                  }
                }
            }
          )
        }
        None
      }
      val newOrderBy = cur.orderBy.flatMap(o => {
        if (newLocalFilters.isEmpty &&
            newLocalGroupBy.isEmpty) {
          val mapped = o.keys.map(f => (getSupportedExpr(f._1, Onions.OPE), f._2))
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
      def processClientComputation(comp: ClientComputation): Map[SqlProj, Int] = {
        comp.projections.map { case (_, p, o, v) =>
          val i = finalProjs.size
          finalProjs += ((p, o, v))
          (p, i)
        }.toMap
      }

      newLocalFilters.foreach { c =>
        localFilterPosMaps += processClientComputation(c)
      }

      newLocalGroupBy.foreach { c =>
        localGroupByPosMaps += processClientComputation(c)
      }

      newLocalOrderBy.foreach { c =>
        localOrderByPosMaps += processClientComputation(c)
      }

      if (encContext.needsProjections) {
        cur.projections.zipWithIndex.foreach {
          case (ExprProj(e, a, _), idx) =>
            val onion = encContext match {
              case EncProj(o) => o(idx)
              case _ => Onions.ALL
            }
            val ctx =
              if (cur.groupBy.isDefined && !newLocalFilters.isEmpty) {
                ProjUnaggCtx(onion)
              } else {
                ProjCtx(onion)
              }
            rewriteExprForServer(e, ctx) match {
              case (_, _, Some(comps)) =>
                val m = processClientComputation(comps)
                projPosMaps += Right((comps, m))

              case (s, o, None) =>
                val stmtIdx = finalProjs.size
                finalProjs += ((ExprProj(s, a), o, false))
                projPosMaps += Left((stmtIdx, o))
            }
          case (StarProj(_), _) => throw new RuntimeException("TODO: implement me")
        }
      }

      cur.copy(projections = finalProjs.map(_._1).toSeq ++ (if (encContext.needsProjections) Seq.empty else Seq(StarProj())))
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
      finalProjs.map(p => if (p._2 == 0) ((None, p._3)) else ((Some(p._2), p._3)))
    val stage1 =
      newLocalFilters
        .zip(localFilterPosMaps)
        .foldLeft( RemoteSql(cur, tdesc, finalSubqueryRelationPlans.toSeq) : PlanNode ) {
          case (acc, (comp, mapping)) =>
            LocalFilter(comp.mkSqlExpr(mapping),
                        wrapDecryptionNodeMap(acc, mapping),
                        comp.subqueries.map(_._2))
        }

    val stage2 =
      newLocalGroupBy.zip(localGroupByPosMaps).foldLeft( stage1 : PlanNode ) {
        case (acc, (comp, mapping)) =>
          LocalGroupFilter(comp.mkSqlExpr(mapping),
                           wrapDecryptionNodeMap(acc, mapping),
                           comp.subqueries.map(_._2))
      }

    val stage3 = {
      if (!newLocalOrderBy.isEmpty) {
        assert(newLocalOrderBy.size == stmt.orderBy.get.keys.size)
        assert(newLocalOrderBy.size == localOrderByPosMaps.size)

        val skipDecryptPos = newLocalOrderBy.map {
          case cc @ ClientComputation(expr, proj, sub) =>
            (proj.size == 1 && proj.head._3 == Onions.OPE &&
             proj.head._1 == expr && sub.isEmpty)
        }

        val decryptPos = localOrderByPosMaps.zip(skipDecryptPos).flatMap {
          case (m, false) => m.values.toSeq
          case (_, true) => Seq.empty
        }

        val orderPos =
          (finalProjs.size until finalProjs.size + stmt.orderBy.get.keys.size).toSeq
        val orderTrfms = newLocalOrderBy.zip(localOrderByPosMaps).map {
          case (comp, mapping) => Right(comp.mkSqlExpr(mapping))
        }

        val allTrfms = (0 until finalProjs.size).map { i => Left(i) } ++ orderTrfms
        LocalOrderBy(
          orderPos.zip(stmt.orderBy.get.keys).map { case (p, (_, t)) => (p, t) },
          LocalTransform(allTrfms,
            if (!decryptPos.isEmpty) LocalDecrypt(decryptPos, stage2) else stage2))
      } else {
        stage2
      }
    }

    val stage4 =
      newLocalLimit.map(l => LocalLimit(l, stage3)).getOrElse(stage3)

    // final stage
    // 1) do all remaining decryptions to answer all the projections
    val m = (
      projPosMaps.flatMap {
        case Right((_, m)) => Some(m.values)
        case _ => None
      }.foldLeft(Seq.empty : Seq[Int])(_++_) ++ {
        projPosMaps.flatMap {
          case Left((p, o)) if o != 0 =>
            Some(p)
          case _ => None
        }
      }
    ).toSeq

    val s0 = if (m.isEmpty) stage4 else wrapDecryptionNodeSeq(stage4, m)

    // 2) now do a final transformation
    val trfms = projPosMaps.map {
      case Right((comp, mapping)) =>
        assert(comp.subqueries.isEmpty)
        Right(comp.mkSqlExpr(mapping))
      case Left((p, _)) => Left(p)
    }

    def checkTransforms(trfms: Seq[Either[Int, SqlExpr]]): Boolean = {
      trfms.zipWithIndex.foldLeft(true) {
        case (acc, (Left(p), idx)) => acc && p == idx
        case (acc, (Right(_), _)) => false
      }
    }

    // if trfms describes purely an identity transform, we can omit it
    val s1 =
      if (trfms.size == s0.tupleDesc.size && checkTransforms(trfms)) s0 else LocalTransform(trfms, s0)

    encContext match {
      case PreserveOriginal => s1
      case PreserveCardinality => stage4 // don't care about projections
      case EncProj(o) =>
        assert(s1.tupleDesc.size == o.size)

        // special case if stage4 is usuable
        val s4td = stage4.tupleDesc

        if (s4td.zip(o).filter {
              case ((Some(x), _), y) => x == y
              case _ => false
            }.size == s4td.size) {
          stage4
        } else {
          val dec =
            s1.tupleDesc.zip(o).zipWithIndex.flatMap {
              case (((Some(x), _), y), _) if x == y => Seq.empty
              case (((Some(x), _), y), i) if x != y => Seq(i)
              case _                                => Seq.empty
            }

          val enc =
            s1.tupleDesc.zip(o).zipWithIndex.flatMap {
              case (((Some(x), _), y), _) if x == y => Seq.empty
              case (((Some(x), _), y), i) if x != y => Seq((i, y))
              case ((_, y), i)                      => Seq((i, y))
            }

          val first = if (dec.isEmpty) s1 else LocalDecrypt(dec, s1)
          val second = if (enc.isEmpty) first else LocalEncrypt(enc, first)
          second
        }
    }
  }

  // if we want to answer e all on the server (modulo some decryption),
  // return the set of expressions and the corresponding bitmask of acceptable
  // onions (that is, any of the onions given is sufficient for a server side
  // rewrite), such that pre-computation is minimal
  private def getPotentialCryptoOpts(e: Node): Option[Seq[(SqlExpr, Int)]] = {
    def pickOne(o: Int): Int = {
      assert(o != 0)
      var i = 0x1
      while (i < 0x70000000) {
        if ((o & i) != 0) return i
        i = i << 1
      }
      assert(false)
      0
    }
    getPotentialCryptoOpts0(e, Onions.ALL).map(_.map(x => (x._1, pickOne(x._2))))
  }

  private def getPotentialCryptoOpts0(e: Node, constraints: Int): Option[Seq[(SqlExpr, Int)]] = {
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
      case eq: EqualityLike =>
        (eq.lhs, eq.rhs) match {
          case (lhs, rhs) if rhs.isLiteral =>
            getPotentialCryptoOpts0(lhs, Onions.DET)
          case (lhs, rhs) if lhs.isLiteral =>
            getPotentialCryptoOpts0(rhs, Onions.DET)
          case (lhs, Subselect(SelectStmt(Seq(ExprProj(expr: SqlAgg, _, _)), _, _, _, _, _, _), _)) =>
            specialCaseExprOpSubselect(lhs, expr)
          case (Subselect(SelectStmt(Seq(ExprProj(expr: SqlAgg, _, _)), _, _, _, _, _, _), _), rhs) =>
            specialCaseExprOpSubselect(rhs, expr)
          case (lhs, rhs) =>
            CollectionUtils.optAnd2(
              getPotentialCryptoOpts0(lhs, Onions.DET),
              getPotentialCryptoOpts0(rhs, Onions.DET)).map { case (l, r) => l ++ r }
        }

      case ieq: InequalityLike =>
        (ieq.lhs, ieq.rhs) match {
          case (lhs, rhs) if rhs.isLiteral =>
            getPotentialCryptoOpts0(lhs, Onions.OPE)
          case (lhs, rhs) if lhs.isLiteral =>
            getPotentialCryptoOpts0(rhs, Onions.OPE)
          case (lhs, Subselect(SelectStmt(Seq(ExprProj(expr: SqlAgg, _, _)), _, _, _, _, _, _), _)) =>
            specialCaseExprOpSubselect(lhs, expr)
          case (Subselect(SelectStmt(Seq(ExprProj(expr: SqlAgg, _, _)), _, _, _, _, _, _), _), rhs) =>
            specialCaseExprOpSubselect(rhs, expr)
          case (lhs, rhs) =>
            CollectionUtils.optAnd2(
              getPotentialCryptoOpts0(lhs, Onions.OPE),
              getPotentialCryptoOpts0(rhs, Onions.OPE)).map { case (l, r) => l ++ r }
        }

      case Like(lhs, rhs, _, _) if rhs.isLiteral =>
        getPotentialCryptoOpts0(lhs, Onions.SWP)
      case Like(lhs, rhs, _, _) if lhs.isLiteral =>
        getPotentialCryptoOpts0(rhs, Onions.SWP)
      case Like(lhs, rhs, _, _) =>
        CollectionUtils.optAnd2(
          getPotentialCryptoOpts0(lhs, Onions.SWP),
          getPotentialCryptoOpts0(rhs, Onions.SWP)).map { case (l, r) => l ++ r }

      case Min(expr, _) =>
        getPotentialCryptoOpts0(expr, Onions.OPE)

      case Max(expr, _) =>
        getPotentialCryptoOpts0(expr, Onions.OPE)

      case Sum(expr, _, _) =>
        getPotentialCryptoOpts0(expr, Onions.HOM)

      case Avg(expr, _, _) =>
        getPotentialCryptoOpts0(expr, Onions.HOM)

      case CountStar(_) => Some(Seq.empty)

      case SqlOrderBy(keys, _) =>
        CollectionUtils.optSeq(
          keys.map(x => getPotentialCryptoOpts0(x._1, Onions.OPE))).map(_.flatten)

      case CaseExprCase(cond, expr, _) =>
        CollectionUtils.optAnd2(
          getPotentialCryptoOpts0(cond, Onions.ALL),
          getPotentialCryptoOpts0(expr, constraints)).map { case (l, r) => l ++ r }

      case CaseWhenExpr(cases, default, _) =>
        default.flatMap { d =>
          CollectionUtils.optSeq(
            cases.map(c => getPotentialCryptoOpts0(c, constraints)) ++
            Seq(getPotentialCryptoOpts0(d, constraints))).map(_.flatten)
        }.orElse {
          CollectionUtils.optSeq(
            cases.map(c => getPotentialCryptoOpts0(c, constraints))).map(_.flatten)
        }

      case In(e, s, _, _) =>
        CollectionUtils.optSeq(
          (Seq(e) ++ s).map(x => getPotentialCryptoOpts0(x, Onions.DET))).map(_.flatten)

      case f : FieldIdent => Some(Seq((f, constraints)))

      case e : SqlExpr if e.isLiteral => Some(Seq.empty)

      case e : SqlExpr if e.getPrecomputableRelation.isDefined => Some(Seq((e, constraints)))

      case _ => None
    }
  }

  def generateCandidatePlans(stmt: SelectStmt): Seq[PlanNode] = {
    val o = generateOnionSets(stmt)
    val perms = CollectionUtils.powerSetMinusEmpty(o)
    // merge all perms, then unique
    val candidates = perms.map(p => OnionSet.merge(p)).toSet.toSeq
    //println("size(candidates) = " + candidates.size)
    //println("candidates = " + candidates)
    def fillOnionSet(o: OnionSet): OnionSet = {
      o.complete(stmt.ctx.defns)
    }
    candidates.map(fillOnionSet).map(o => generatePlanFromOnionSet(stmt, o)).toSet.toSeq
  }

  def generateOnionSets(stmt: SelectStmt): Seq[OnionSet] = {

    def traverseContext(
      start: Node,
      ctx: Context,
      bootstrap: Seq[OnionSet],
      selectFn: (SelectStmt, Seq[OnionSet]) => Seq[OnionSet]): Seq[OnionSet] = {

      var workingSet : Seq[OnionSet] = bootstrap

      def add(exprs: Seq[(SqlExpr, Int)]): Boolean = {
        val e0 = exprs.map { case (e, o) => findOnionableExpr(e).map(e0 => (e0, o)) }.flatten
        if (e0.size == exprs.size) {
          e0.foreach { case ((_, t, e), o) => workingSet.foreach(_.add(t, e, o)) }
          true
        } else false
      }

      def procNode(n: Node) = {
        getPotentialCryptoOpts(n) match {
          case Some(exprs) => add(exprs)
          case None =>
            n match {
              case Subselect(ss, _) =>
                workingSet = selectFn(ss, workingSet)
                true
              case _ => false
            }
        }
      }

      topDownTraverseContext(start, ctx)(negate {
        case e: SqlExpr =>
          val clauses = splitTopLevelClauses(e)
          assert(!clauses.isEmpty)
          if (clauses.size == 1) {
            procNode(clauses.head)
          } else {
            val sets = clauses.flatMap(c => traverseContext(c, ctx, bootstrap, selectFn))
            workingSet = workingSet.map { ws =>
              sets.foldLeft(ws) { case (acc, elem) => acc.merge(elem) }
            }
            true
          }
        case e => procNode(e)
      })

      workingSet
    }

    def buildForSelectStmt(stmt: SelectStmt, bootstrap: Seq[OnionSet]): Seq[OnionSet] = {
      val SelectStmt(p, r, f, g, o, _, ctx) = stmt
      val s0 = {
        var workingSet = bootstrap.map(_.copy)
        p.foreach { e =>
          workingSet = traverseContext(e, ctx, workingSet, buildForSelectStmt)
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
      val s2 = f.map(e => traverseContext(e, ctx, bootstrap.map(_.copy), buildForSelectStmt))
      val s3 = g.map(e => traverseContext(e, ctx, bootstrap.map(_.copy), buildForSelectStmt))
      val s4 = o.map(e => traverseContext(e, ctx, bootstrap.map(_.copy), buildForSelectStmt))
      (s0 ++ s1.getOrElse(Seq.empty) ++ s2.getOrElse(Seq.empty) ++
      s3.getOrElse(Seq.empty) ++ s4.getOrElse(Seq.empty)).filterNot(_.isEmpty)
    }

    buildForSelectStmt(stmt, Seq(new OnionSet))
  }
}
