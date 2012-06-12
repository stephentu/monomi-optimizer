package edu.mit.cryptdb

import scala.collection.mutable.{
  ArrayBuffer, HashMap, HashSet, Seq => MSeq, Map => MMap }

trait Generator extends Traversals
  with PlanTraversals with Transformers with PlanTransformers with Timer {

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

  private val stopGoing = (None, false)

  private def negate(f: Node => Boolean): Node => Boolean = (n: Node) => { !f(n) }

  private def resolveAliases(e: SqlExpr): SqlExpr = e.resolveAliases

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
        case FieldIdent(_, _, ColumnSymbol(relation, name, ctx, _), _) =>
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

  private def encTblName(t: String) = t + "$enc"

  private def verifyPlanNode[P <: PlanNode](p: P): P = {
    val _unused = p.tupleDesc // has many sanity checks
    p
  }

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

  private def buildHomGroupPreference(uses: Seq[Seq[HomDesc]]):
    Map[String, Seq[Int]] = {
    // filter out all non-explicit hom-descriptors
    val uses0 = uses.filterNot(_.isEmpty).flatten

    // build counts for each unique descriptor
    val counts = new HashMap[String, MMap[Int, Int]]
    uses0.foreach { hd =>
      val m = counts.getOrElseUpdate(hd.table, new HashMap[Int, Int])
      m.put(hd.group, m.getOrElse(hd.group, 0) + 1)
    }

    counts.map {
      case (k, vs) => (k, vs.toSeq.sortWith(_._2 < _._2).map(_._1)) }.toMap
  }

  // returns a mapping of DependentFieldPlaceholder -> FieldIdent which was
  // replaced in stmt
  private def rewriteOuterReferences(stmt: SelectStmt):
    (SelectStmt, Seq[(DependentFieldPlaceholder, FieldIdent)]) = {
    val buf = new ArrayBuffer[(DependentFieldPlaceholder, FieldIdent)]
    (topDownTransformation(stmt) {
      case fi @ FieldIdent(_, _, ColumnSymbol(_, _, ctx, _), _) =>
        if (ctx.isParentOf(stmt.ctx)) {
          val rep = DependentFieldPlaceholder(buf.size)
          buf += ((rep, fi))
          replaceWith(rep)
        } else stopGoing
      case FieldIdent(_, _, _: ProjectionSymbol, _) =>
        // TODO: don't know how to handle this
        // also SQL might not even allow this
        throw new RuntimeException("TODO: implement me")
      case _ => keepGoing
    }.asInstanceOf[SelectStmt], buf.toSeq)
  }

  private def hasOuterReferences(stmt: SelectStmt): Boolean = {
    var has = false
    topDownTraversal(stmt) {
      case fi @ FieldIdent(_, _, ColumnSymbol(_, _, ctx, _), _) =>
        if (ctx.isParentOf(stmt.ctx)) has = true
        !has
      case _ => !has
    }
    has
  }

  case class RewriteAnalysisContext(subrels: Map[String, PlanNode],
                                    homGroupPreferences: Map[String, Seq[Int]],
                                    groupKeys: Map[Symbol, (FieldIdent, OnionType)])

  case class HomDesc(table: String, group: Int, pos: Int)

  object CompProjMapping {
    final val empty = CompProjMapping(Map.empty, Map.empty)
  }

  case class CompProjMapping(
    projMap: Map[SqlProj, Int], subqueryProjMap: Map[SqlProj, Int]) {
    def values: Seq[Int] = projMap.values.toSeq ++ subqueryProjMap.values.toSeq
    def size: Int = values.size

    // creates a new update
    def update(idx: Map[Int, Int]): CompProjMapping = {
      CompProjMapping(
        projMap.map { case (p, i) => (p, idx(i)) }.toMap,
        subqueryProjMap.map { case (p, i) => (p, idx(i)) }.toMap)
    }
  }

  private def encLiteral(e: SqlExpr, o: Int, keyConstraint: (Int, DataType)): SqlExpr = {
    assert(BitUtils.onlyOne(o))
    assert(e.isLiteral)
    // acts as a marker for downstream phases
    FunctionCall(
      "encrypt",
      Seq(e, IntLiteral(o), MetaFieldIdent(keyConstraint._1, keyConstraint._2)))
  }

  // names are unique over a generator instance

  private val _subselectMaterializeNamePrefix = "_subselect$"
  private val _subselectMaterializeNames = new NameGenerator(_subselectMaterializeNamePrefix)

  private val _hiddenNamePrefix = "_hidden$"
  private val _hiddenNames = new NameGenerator(_hiddenNamePrefix)

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

    val subRelnGen = new NameGenerator("subrelation$")

    // empty seq signifies wildcard
    def getSupportedHOMRowDescExpr(e: SqlExpr, subrels: Map[String, PlanNode]):
      Option[(SqlExpr, Seq[HomDesc])] = {
      if (e.isLiteral) {
        // TODO: coerce to integer?
        Some((FunctionCall("hom_row_desc_lit", Seq(e)), Seq.empty))
      } else {
        def procSubqueryRef(e: SqlExpr): Option[(SqlExpr, Seq[HomDesc])] = {
          e match {
            case fi @ FieldIdent(_, _, ColumnSymbol(relation, name, ctx, _), _)
              if ctx.relations(relation).isInstanceOf[SubqueryRelation] =>
              val idx =
                ctx
                  .relations(relation)
                  .asInstanceOf[SubqueryRelation]
                  .stmt.ctx.lookupNamedProjectionIndex(name).get
              // TODO: what do we do if the relation tupleDesc is in vector context
              assert(!subrels(relation).tupleDesc(idx).vectorCtx)
              val po = subrels(relation).tupleDesc(idx).onion
              if ((po.onion & Onions.HOM_ROW_DESC) != 0) {
                findOnionableExpr(e).flatMap { case (_, t, x) =>
                  val h = onionSet.lookupPackedHOM(t, x)
                  if (h.isEmpty) {
                    None
                  } else {
                    Some((fi.copyWithContext(null).asInstanceOf[FieldIdent].copy(qualifier = Some(relation)),
                          h.map { case (g, p) => HomDesc(t, g, p) }))
                  }
                }
              } else None
            case _ => None
          }
        }
        findOnionableExpr(e).flatMap { case (r, t, x) =>
          e match {
            case fi @ FieldIdent(_, _, ColumnSymbol(relation, name0, ctx, _), _)
              if ctx.relations(relation).isInstanceOf[SubqueryRelation] => procSubqueryRef(e)
            case _ =>
              val qual = if (r == t) encTblName(t) else r
              val h = onionSet.lookupPackedHOM(t, x)
              if (h.isEmpty) None
              else {
                Some((FieldIdent(Some(qual), "row_id"), h.map { case (g, p) => HomDesc(t, g, p) }))
              }
          }
        }
      }
    }

    def getSupportedExprConstraintAware(
      e: SqlExpr, o: Int,
      subrels: Map[String, PlanNode],
      groupKeys: Map[Symbol, (FieldIdent, OnionType)],
      aggContext: Boolean,
      keyConstraint: Option[(Int, DataType)]): Option[(SqlExpr, OnionType)] = {
      // need to check if we are constrained by group keys
      e match {
        case FieldIdent(_, _, sym, _) if aggContext =>
          groupKeys.get(sym) match {
            case Some((expr, o0)) if o0.isOneOf(o) => Some((expr, o0))
            case Some(_)                           => None // cannot support
            case None                              =>
              getSupportedExpr(e, o, subrels, keyConstraint)
          }
        case _ => getSupportedExpr(e, o, subrels, keyConstraint)
      }
    }

    // return a *server-side expr* which is equivalent to e under onion o,
    // if possible. otherwise return None. o can be a bitmask of allowed
    // onions. use the return value to determine which onion was chosen.
    //
    // handles literals properly
    def getSupportedExpr(
      e: SqlExpr, o: Int,
      subrels: Map[String, PlanNode],
      keyConstraint: Option[(Int, DataType)]):
      Option[(SqlExpr, OnionType)] = {

      e match {
        case e if e.isLiteral =>
          // easy case
          Onions.pickOne(o) match {
            case Onions.PLAIN => Some((e.copyWithContext(null).asInstanceOf[SqlExpr], PlainOnion))
            case o0           =>
              Some((encLiteral(e, o0, keyConstraint.get),
                    OnionType.buildIndividual(o0))) // TODO: encryption
          }

        case d: DependentFieldPlaceholder =>
          val o0 = Onions.pickOne(o)
          Some((d.bind(o0), OnionType.buildIndividual(o0)))

        case _ =>
          def procSubqueryRef(e: SqlExpr) = {
            e match {
              case fi @ FieldIdent(_, _, ColumnSymbol(relation, name, ctx, _), _)
                if ctx.relations(relation).isInstanceOf[SubqueryRelation] =>
                val idx =
                  ctx
                    .relations(relation)
                    .asInstanceOf[SubqueryRelation]
                    .stmt.ctx.lookupNamedProjectionIndex(name).get
                // TODO: what do we do if the relation tupleDesc is in vector context
                assert(!subrels(relation).tupleDesc(idx).vectorCtx)
                val po = subrels(relation).tupleDesc(idx).onion
                if ((po.onion & o) != 0) {
                  Some((fi.copyWithContext(null).copy(qualifier = Some(relation)),
                        OnionType.buildIndividual(po.onion)))
                } else None
              case _ => None
            }
          }
          val e0 = findOnionableExpr(e)
          e0.flatMap { case (r, t, x) =>
            e match {
              case fi @ FieldIdent(_, _, ColumnSymbol(relation, name0, ctx, _), _)
                if ctx.relations(relation).isInstanceOf[SubqueryRelation] => procSubqueryRef(e)
              case _ =>
                //println("e = " + e.sql)
                //println("t=(%s), x=(%s)".format(t, x.toString))
                onionSet.lookup(t, x).filter(y => (y._2 & o) != 0).map {
                  case (basename, o0) =>
                    val qual = if (r == t) encTblName(t) else r
                    val choice = Onions.pickOne(o0 & o)
                    val name = basename + "$" + Onions.str(choice)
                    ((FieldIdent(Some(qual), name), OnionType.buildIndividual(choice)))
                }
            }
          }.orElse {
            // TODO: this is hacky -
            // special case- if we looking at a field projection
            // from a subquery relation
            procSubqueryRef(e)
          }
      }
    }

    // ClientComputations leave the result of the expr un-encrypted
    case class ClientComputation
      (/* a client side expr for evaluation locally. the result of the expr is assumed
        * to be un-encrypted */
       expr: SqlExpr,

       /* unmodified, original expression, used later for cost analysis */
       origExpr: SqlExpr,

       /* additional encrypted projections needed for conjunction. the tuple is as follows:
        *   ( (original) expr from the client expr which will be replaced with proj,
        *     the actual projection to append to the *server side* query,
        *     the encryption onion which is being projected from the server side query,
        *     whether or not the proj is in vector ctx ) */
       projections: Seq[(SqlExpr, SqlProj, OnionType, Boolean)],

       subqueryProjections: Seq[(SqlExpr, SqlProj, OnionType, Boolean)],

       /* additional subqueries needed for conjunction. the tuple is as follows:
        *   ( (original) SelectStmt from the conjunction which will be replaced with PlanNode,
        *     the PlanNode to execute )
        */
       subqueries: Seq[(Subselect, PlanNode, Seq[(DependentFieldPlaceholder, FieldIdent)])]
      ) {

      /** Assumes both ClientComputations are conjunctions, and merges them into a single
       * computation */
      def mergeConjunctions(that: ClientComputation): ClientComputation = {
        ClientComputation(
          And(this.expr, that.expr),
          And(this.origExpr, that.origExpr),
          this.projections ++ that.projections,
          this.subqueryProjections ++ that.subqueryProjections,
          this.subqueries ++ that.subqueries)
      }

      // makes the client side expression SQL to go in a LocalTransform node
      def mkSqlExpr(mappings: CompProjMapping): SqlExpr = {
        val pmap  = projections.map(x => (x._1, x._2)).toMap
        val spmap = subqueryProjections.map(x => (x._1, x._2)).toMap
        val smap  = subqueries.zipWithIndex.map { case ((s, _, ss), i) =>
          // arguments to this subquery (as tuple positions)
          val args =
            ss.map(_._2).map(x => TuplePosition(mappings.subqueryProjMap(spmap(x))))
          (s, (i, args))
        }.toMap

        def testExpr(expr0: SqlExpr): Option[SqlExpr] = expr0 match {
          case Exists(s: Subselect, _) =>
            val r = smap(s)
            Some(ExistsSubqueryPosition(r._1, r._2))
          case s: Subselect            =>
            val r = smap(s)
            Some(SubqueryPosition(r._1, r._2))
          case e                       =>
            pmap.get(e).map { p => TuplePosition(mappings.projMap(p)) }
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

    val subselectNodes = new HashMap[String, (PlanNode, SelectStmt)]

    def addSubselectAndReturnPlaceholder(p: PlanNode, origSS: SelectStmt): SqlExpr = {
      assert(!hasOuterReferences(origSS))
      val id = _subselectMaterializeNames.uniqueId()
      subselectNodes += (id -> (p, origSS))
      NamedSubselectPlaceholder(id)
    }

    def mergeNamedSubselects(m: Map[String, (PlanNode, SelectStmt)]): Unit = {
      // TODO: assert no name clashes
      subselectNodes ++= m
    }

    val finalSubqueryRelationPlans = new ArrayBuffer[(RemoteMaterialize, SelectStmt)]

    def mergeSubrelations(b: Seq[(RemoteMaterialize, SelectStmt)]): Unit = {
      finalSubqueryRelationPlans ++= b
    }

    def mergeRemoteSql(rs: RemoteSql): Unit = {
      mergeSubrelations(rs.subrelations)
      mergeNamedSubselects(rs.namedSubselects)
    }

    // local filter + name of concrete {table,subquery} relations
    // to nullify (named in this context) if the filter fails (none if inner join)
    val newLocalJoinFilters
      = new ArrayBuffer[(ClientComputation, Set[String], SqlRelation)]
    val localJoinFilterPosMaps = new ArrayBuffer[CompProjMapping]

    // are all join clauses resolved on the server?
    def checkJoinClausesOnServer: Boolean = {
      newLocalJoinFilters.isEmpty
    }

    // can we process the WHERE clause on the server
    // does NOT imply that all join clauses have been resolved, just that
    // no OUTER join clauses are un-resolved
    def checkCanDoFilterOnServer: Boolean = {
      newLocalJoinFilters.filterNot(_._2.isEmpty).isEmpty
    }

    val newLocalFilters = new ArrayBuffer[ClientComputation]
    val localFilterPosMaps = new ArrayBuffer[CompProjMapping]

    // is the filter (where) clause resolved on the server.
    // doesn't imply all join clauses are resolved
    def checkFilterClauseOnServer: Boolean = {
      newLocalFilters.isEmpty
    }

    // can we process the GROUP BY clause on the server
    //
    // NOTE: currently, we assume that if we *CAN* execute the filter we
    // can also execute the group by. This is because if we can execute
    // the filter, then even if some filter clauses cannot be answered on the
    // server, we can still apply a LocalGroupFilter
    def checkCanDoGroupByOnServer: Boolean = {
      checkCanDoFilterOnServer
    }

    // corresponds 1-to-1 w/ the group by keys
    val newLocalGroupBy = new ArrayBuffer[ClientComputation]
    val localGroupByPosMaps = new ArrayBuffer[CompProjMapping]

    val newLocalGroupByHaving = new ArrayBuffer[ClientComputation]
    val localGroupByHavingPosMaps = new ArrayBuffer[CompProjMapping]

    // is the group by cluase resolved on the server
    def checkGroupByOnServer: Boolean = {
      newLocalGroupBy.isEmpty && newLocalGroupByHaving.isEmpty
    }

    def checkCanDoOrderByOnServer: Boolean = {
      checkGroupByOnServer
    }

    // left is position in (original) projection to order by,
    // right is client comp
    //
    // NOTE: the position is NOT a position in the final projection list, but a logical
    //       projection position. this assumes no STAR projections
    val newLocalOrderBy = new ArrayBuffer[Either[Int, ClientComputation]]
    val localOrderByPosMaps = new ArrayBuffer[CompProjMapping]

    def checkOrderByOnServer: Boolean = {
      newLocalOrderBy.isEmpty
    }

    def checkCanDoLimitOnServer: Boolean = {
      // can only do limit if everything before is good to go
      // this is kind of conservative (but correct)
      checkOrderByOnServer &&
      checkGroupByOnServer &&
      checkFilterClauseOnServer &&
      checkJoinClausesOnServer
    }

    var newLocalLimit: Option[Int] = None

    def checkLimitOnServer: Boolean = {
      newLocalLimit.isEmpty
    }

    // these correspond 1 to 1 with the original projections
    val projPosMaps =
      new ArrayBuffer[Either[(Int, OnionType), (ClientComputation, CompProjMapping)]]

    // these correspond 1 to 1 with the new projections in the encrypted
    // re-written query
    // values are:
    //    (orig expr,
    //     optional orig proj name,
    //     projection,
    //     what onion it is projected in,
    //     vector ctx)
    val finalProjs =
      new ArrayBuffer[(SqlExpr, Option[String], SqlProj, OnionType, Boolean)]

    case class RewriteContext(
      onions: Seq[Int],
      aggContext: Boolean, // aggContext means its OK for agg expressions to appear
      respectStructure: Boolean, // this only has any effect if the expr
                                 // cannot be answered on the server side and we need
                                 // to project fields for client evaluation. if true,
                                 // the projected fields respect the structure of the
                                 // expression (fields within aggs will be group_concat()-ed,
                                 // fields outside will be regularly projected). if false, then
                                 // the fields are never group_concat()-ed, regardless.
      keyConstraint: Option[(Int, DataType)] = None // the field on the other side of a binary op
      ) {

      def this(onion: Int, aggContext: Boolean, respectStructure: Boolean) =
        this(Onions.toSeq(onion), aggContext, respectStructure)

      assert(!onions.isEmpty)
      assert(onions.filterNot(BitUtils.onlyOne).isEmpty)

      def inClear: Boolean = testOnion(Onions.PLAIN)
      def testOnion(o: Int): Boolean = !onions.filter { x => (x & o) != 0 }.isEmpty
      def restrict: RewriteContext = copy(onions = Seq(onions.head))

      def restrictTo(o: Int) = new RewriteContext(o, aggContext, respectStructure)
      def withKey(expr: SqlExpr): RewriteContext = expr match {

        // this is the common case- where we are comparing to a given column
        case FieldIdent(_, _, cs : ColumnSymbol, _) =>
          copy(keyConstraint = Some((cs.fieldPosition, cs.tpe)))

        // this is the case where we compare to a precomputed field
        case e if e.getPrecomputableRelation.isDefined =>
          copy(keyConstraint = Some((0 /* signifies pre-computation */, e.getType.tpe)))

        // TODO: subselects?
        case _ => this
      }
    }

    sealed abstract trait ServerRewriteMode
    case object ServerAll extends ServerRewriteMode
    case object ServerProj extends ServerRewriteMode
    case object ServerNone extends ServerRewriteMode

    def rewriteExprForServer(
      expr: SqlExpr, rewriteCtx: RewriteContext,
      analysis: RewriteAnalysisContext, serverRewriteMode: ServerRewriteMode):
      Either[(SqlExpr, OnionType), (Option[(SqlExpr, OnionType)], ClientComputation)] = {

      // this is just a placeholder in the tree
      val cannotAnswerExpr = replaceWith(IntLiteral(1))

      def doTransform(e: SqlExpr, curRewriteCtx: RewriteContext):
        Either[(SqlExpr, OnionType), ClientComputation] = {

        def rewriteExprNonRecursive(e: SqlExpr, curRewriteCtx: RewriteContext):
          Option[(SqlExpr, OnionType)] = {
          curRewriteCtx
            .onions
            .foldLeft( None : Option[(SqlExpr, OnionType)] ) { case (acc, o) =>
              acc.orElse {
                assert(BitUtils.onlyOne(o))
                if (o == Onions.HOM_ROW_DESC) {
                  getSupportedHOMRowDescExpr(e, analysis.subrels)
                    .map { case (expr, hds) =>
                      (expr, HomRowDescOnion(hds.map(_.table).toSet.head))
                    }
                } else {
                  getSupportedExprConstraintAware(
                    e, o, analysis.subrels,
                    analysis.groupKeys, curRewriteCtx.aggContext,
                    curRewriteCtx.keyConstraint)
                }
              }
            }
        }

        def doTransformServer(e: SqlExpr, curRewriteCtx: RewriteContext):
          Option[(SqlExpr, OnionType)] = {

          val onionRetVal = new SetOnce[OnionType] // TODO: FIX THIS HACK
          var _exprValid = true
          def bailOut = {
            _exprValid = false
            cannotAnswerExpr
          }

          val newExpr = topDownTransformation(e) {

            case Or(l, r, _) if curRewriteCtx.inClear =>
              onionRetVal.set(OnionType.buildIndividual(Onions.PLAIN))
              CollectionUtils.optAnd2(
                doTransformServer(l, curRewriteCtx.restrictTo(Onions.PLAIN)),
                doTransformServer(r, curRewriteCtx.restrictTo(Onions.PLAIN))).map {
                  case ((l0, _), (r0, _)) => replaceWith(Or(l0, r0))
                }.getOrElse(bailOut)

            case And(l, r, _) if curRewriteCtx.inClear =>
              onionRetVal.set(OnionType.buildIndividual(Onions.PLAIN))
              CollectionUtils.optAnd2(
                doTransformServer(l, curRewriteCtx.restrictTo(Onions.PLAIN)),
                doTransformServer(r, curRewriteCtx.restrictTo(Onions.PLAIN))).map {
                  case ((l0, _), (r0, _)) => replaceWith(And(l0, r0))
                }.getOrElse(bailOut)

            case eq: EqualityLike if curRewriteCtx.inClear =>
              onionRetVal.set(OnionType.buildIndividual(Onions.PLAIN))

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
                          case rs @ RemoteSql(q0, _, _, _) =>
                            mergeRemoteSql(rs)
                            Some(eq.copyWithChildren(expr, Subselect(q0)))
                          // TODO: use named subselect
                          case _                        => None
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
                    case (r1 @ RemoteSql(q0p, _, _, _), r2 @ RemoteSql(q1p, _, _, _)) =>
                      mergeRemoteSql(r1)
                      mergeRemoteSql(r2)
                      replaceWith(eq.copyWithChildren(Subselect(q0p), Subselect(q1p)))
                    case _ =>
                      (spDETs(0), spDETs(1)) match {
                        case (r1 @ RemoteSql(q0p, _, _, _), r2 @ RemoteSql(q1p, _, _, _)) =>
                          mergeRemoteSql(r1)
                          mergeRemoteSql(r2)
                          replaceWith(eq.copyWithChildren(Subselect(q0p), Subselect(q1p)))
                        case _ =>
                          (spOPEs(0), spOPEs(1)) match {
                            case (r1 @ RemoteSql(q0p, _, _, _), r2 @ RemoteSql(q1p, _, _, _)) =>
                              mergeRemoteSql(r1)
                              mergeRemoteSql(r2)
                              replaceWith(eq.copyWithChildren(Subselect(q0p), Subselect(q1p)))
                            case _ =>
                              // TODO: use named subselect
                              bailOut
                          }
                      }
                  }
                case (ss @ Subselect(_, _), rhs) => handleOneSubselect(ss, rhs)
                case (lhs, ss @ Subselect(_, _)) => handleOneSubselect(ss, lhs)
                case (lhs, rhs) =>
                  CollectionUtils.optAnd2(
                    doTransformServer(lhs, curRewriteCtx.restrictTo(Onions.PLAIN).withKey(rhs)),
                    doTransformServer(rhs, curRewriteCtx.restrictTo(Onions.PLAIN).withKey(lhs)))
                  .orElse(
                    CollectionUtils.optAnd2(
                      doTransformServer(lhs, curRewriteCtx.restrictTo(Onions.DET).withKey(rhs)),
                      doTransformServer(rhs, curRewriteCtx.restrictTo(Onions.DET).withKey(lhs))))
                  .orElse(
                    CollectionUtils.optAnd2(
                      doTransformServer(lhs, curRewriteCtx.restrictTo(Onions.OPE).withKey(rhs)),
                      doTransformServer(rhs, curRewriteCtx.restrictTo(Onions.OPE).withKey(lhs))))
                  .map {
                    case ((lfi, _), (rfi, _)) =>
                      replaceWith(eq.copyWithChildren(lfi, rfi))
                  }.getOrElse(bailOut)
              }

            // TODO: don't copy so much code from EqualityLike
            case ieq: InequalityLike if curRewriteCtx.inClear =>
              onionRetVal.set(OnionType.buildIndividual(Onions.PLAIN))

              // handles "ss op expr" (flip means expr op ss)
              def handleOneSubselect(ss: Subselect, expr: SqlExpr, flip: Boolean) = {
                assert(!expr.isInstanceOf[Subselect])
                val onions = Seq(Onions.PLAIN, Onions.OPE)
                onions.foldLeft(None : Option[SqlExpr]) {
                  case (acc, onion) =>
                    acc.orElse {
                      val e0 = doTransformServer(expr, curRewriteCtx.restrictTo(onion))
                      e0.flatMap { case (expr, _) =>
                        generatePlanFromOnionSet0(
                          ss.subquery, onionSet, EncProj(Seq(onion), true)) match {
                          case rs @ RemoteSql(q0, _, _, _) =>
                            mergeRemoteSql(rs)
                            if (flip) Some(ieq.copyWithChildren(expr, Subselect(q0)))
                            else      Some(ieq.copyWithChildren(Subselect(q0), expr))
                          case p =>
                            val repr = addSubselectAndReturnPlaceholder(p, ss.subquery)
                            if (flip) Some(ieq.copyWithChildren(expr, repr))
                            else      Some(ieq.copyWithChildren(repr, expr))
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
                    case (r1 @ RemoteSql(q0p, _, _, _), r2 @ RemoteSql(q1p, _, _, _)) =>
                      mergeRemoteSql(r1)
                      mergeRemoteSql(r2)
                      replaceWith(ieq.copyWithChildren(Subselect(q0p), Subselect(q1p)))
                    case _ =>
                      (spOPEs(0), spOPEs(1)) match {
                        case (r1 @ RemoteSql(q0p, _, _, _), r2 @ RemoteSql(q1p, _, _, _)) =>
                          mergeRemoteSql(r1)
                          mergeRemoteSql(r2)
                          replaceWith(ieq.copyWithChildren(Subselect(q0p), Subselect(q1p)))
                        case _ => bailOut
                      }
                  }
                case (ss @ Subselect(_, _), rhs) => handleOneSubselect(ss, rhs, false)
                case (lhs, ss @ Subselect(_, _)) => handleOneSubselect(ss, lhs, true)
                case (lhs, rhs) =>
                  CollectionUtils.optAnd2(
                    doTransformServer(lhs, curRewriteCtx.restrictTo(Onions.PLAIN).withKey(rhs)),
                    doTransformServer(rhs, curRewriteCtx.restrictTo(Onions.PLAIN).withKey(lhs)))
                  .orElse(
                    CollectionUtils.optAnd2(
                      doTransformServer(lhs, curRewriteCtx.restrictTo(Onions.OPE).withKey(rhs)),
                      doTransformServer(rhs, curRewriteCtx.restrictTo(Onions.OPE).withKey(lhs))))
                  .map {
                    case ((lfi, _), (rfi, _)) =>
                      replaceWith(ieq.copyWithChildren(lfi, rfi))
                  }.getOrElse(bailOut)
              }

            // TODO: handle subqueries
            case like @ Like(lhs, rhs, _, _) if curRewriteCtx.inClear =>
              onionRetVal.set(OnionType.buildIndividual(Onions.PLAIN))
              CollectionUtils.optAnd2(
                doTransformServer(lhs, curRewriteCtx.restrictTo(Onions.SWP)),
                doTransformServer(rhs, curRewriteCtx.restrictTo(Onions.SWP).withKey(lhs))).map {
                  case ((l0, _), (r0, _)) =>
                    replaceWith(FunctionCall("searchSWP", Seq(l0, r0)))
                }.getOrElse(bailOut)

            // TODO: handle subqueries
            case in @ In(e, s, n, _) if curRewriteCtx.inClear =>
              onionRetVal.set(OnionType.buildIndividual(Onions.PLAIN))
              def tryOnion(o: Int) = {
                val e0 = doTransformServer(e, curRewriteCtx.restrictTo(o))
                val t = s.map { x =>
                  doTransformServer(x, curRewriteCtx.restrictTo(o).withKey(e))
                }
                //println("tryOnion: ")
                //println("  in=" + in.sql)
                //println("  e0=" + e0)
                //println("  t=" + t)
                CollectionUtils.optSeq(Seq(e0) ++ t).map { s0 =>
                  replaceWith(In(s0.head._1, s0.tail.map(_._1), n))
                }
              }
              tryOnion(Onions.DET).orElse(tryOnion(Onions.OPE)).getOrElse {
                s match {
                  case Seq(Subselect(ss, _)) =>
                    // try DET, then OPE, then bailout
                    doTransformServer(e, curRewriteCtx.restrictTo(Onions.DET)).map(x => (x, Onions.DET))
                    .orElse(
                      doTransformServer(e, curRewriteCtx.restrictTo(Onions.OPE)).map(x => (x, Onions.OPE)))
                    .map {
                      case ((e0, _), o) =>
                        val p = generatePlanFromOnionSet0(ss, onionSet, EncProj(Seq(o), true))
                        replaceWith(In(e0, Seq(addSubselectAndReturnPlaceholder(p, ss)), n))
                    }.getOrElse(bailOut)

                  case _ => bailOut
                }
              }

            case not @ Not(e, _) if curRewriteCtx.inClear =>
              onionRetVal.set(OnionType.buildIndividual(Onions.PLAIN))
              doTransformServer(e, curRewriteCtx.restrictTo(Onions.PLAIN))
                .map { case (e0, _) => replaceWith(Not(e0)) }.getOrElse(bailOut)

            case ex @ Exists(ss, _) if curRewriteCtx.inClear =>
              onionRetVal.set(OnionType.buildIndividual(Onions.PLAIN))
              generatePlanFromOnionSet0(ss.subquery, onionSet, PreserveCardinality) match {
                case rs @ RemoteSql(q, _, _, _) =>
                  mergeRemoteSql(rs)
                  replaceWith(Exists(Subselect(q)))
                case _                          => bailOut
              }

            case cs @ CountStar(_) if curRewriteCtx.inClear && curRewriteCtx.aggContext =>
              onionRetVal.set(OnionType.buildIndividual(Onions.PLAIN))
              replaceWith(CountStar())

            case cs @ CountExpr(e, d, _) if curRewriteCtx.inClear && curRewriteCtx.aggContext =>
              onionRetVal.set(OnionType.buildIndividual(Onions.PLAIN))
              doTransformServer(e, RewriteContext(Onions.toSeq(Onions.Countable), false, true))
                .map { case (e0, _) => replaceWith(CountExpr(e0, d)) }.getOrElse(bailOut)

            case m @ Min(f, _) if curRewriteCtx.testOnion(Onions.OPE) && curRewriteCtx.aggContext =>
              onionRetVal.set(OnionType.buildIndividual(Onions.OPE))
              doTransformServer(f, RewriteContext(Seq(Onions.OPE), false, true))
                .map { case (e0, _) => replaceWith(Min(e0)) }.getOrElse(bailOut)

            case m @ Max(f, _) if curRewriteCtx.testOnion(Onions.OPE) && curRewriteCtx.aggContext =>
              onionRetVal.set(OnionType.buildIndividual(Onions.OPE))
              doTransformServer(f, RewriteContext(Seq(Onions.OPE), false, true))
                .map { case (e0, _) => replaceWith(Max(e0)) }.getOrElse(bailOut)

            // TODO: we should do something about distinct
            case s @ Sum(f, d, _) if curRewriteCtx.aggContext =>
              def tryPlain = {
                doTransformServer(f, RewriteContext(Seq(Onions.PLAIN), false, true))
                  .map { case (e0, _) =>
                    onionRetVal.set(OnionType.buildIndividual(Onions.PLAIN))
                    replaceWith(Sum(e0, d))
                  }
              }
              def tryHom = {
                doTransformServer(f, RewriteContext(Seq(Onions.HOM), false, true))
                  .map { case (e0, _) =>
                    onionRetVal.set(OnionType.buildIndividual(Onions.HOM))
                    replaceWith(AggCall("hom_agg", Seq(e0)))
                  }
              }
              if (curRewriteCtx.inClear && curRewriteCtx.testOnion(Onions.HOM)) {
                tryPlain.orElse(tryHom).getOrElse(bailOut)
              } else if (curRewriteCtx.testOnion(Onions.HOM)) {
                tryHom.getOrElse(bailOut)
              } else if (curRewriteCtx.inClear) {
                tryPlain.getOrElse(bailOut)
              } else { bailOut }

            case CaseWhenExpr(cases, default, _) =>
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
                          onionRetVal.set(OnionType.buildIndividual(o))
                          CaseWhenExpr(cases0, Some(d0))
                      }
                    case None =>
                      onionRetVal.set(OnionType.buildIndividual(o))
                      Some(CaseWhenExpr(cases0, None))
                  }
                }
              }

              curRewriteCtx.onions.foldLeft( None : Option[SqlExpr] ) {
                case (acc, onion) => acc.orElse(tryWith(onion))
              }.map(replaceWith).getOrElse(bailOut)

            case e: SqlExpr if e.isLiteral =>
              onionRetVal.set(OnionType.buildIndividual(curRewriteCtx.onions.head))
              curRewriteCtx.onions.head match {
                case Onions.PLAIN => replaceWith(e.copyWithContext(null).asInstanceOf[SqlExpr])
                case o            =>
                  curRewriteCtx.keyConstraint.map {
                    k => replaceWith(encLiteral(e, o, k))
                  }.getOrElse(bailOut)
              }

            case e: SqlExpr =>
              rewriteExprNonRecursive(e, curRewriteCtx).map {
                case (expr, ret) =>
                  onionRetVal.set(ret)
                  replaceWith(expr)
              }.getOrElse(bailOut)

            case e => throw new Exception("should only have exprs under expr clause")
          }.asInstanceOf[SqlExpr]

          if (_exprValid) Some(newExpr, onionRetVal.get.get) else None
        }

        def doTransformClient(e: SqlExpr, curRewriteCtx: RewriteContext):
          ClientComputation = {
          // take care of all subselects first
          val subselects =
            new ArrayBuffer[(Subselect, PlanNode, Seq[(DependentFieldPlaceholder, FieldIdent)])]
          topDownTraversalWithParent(e) {
            case (Some(_: Exists), s @ Subselect(ss, _)) =>
              val (ss0, m) = rewriteOuterReferences(ss)
              val p = generatePlanFromOnionSet0(ss0, onionSet, PreserveCardinality)
              subselects += ((s, p, m))
              false
            case (_, s @ Subselect(ss, _)) =>
              val (ss0, m) = rewriteOuterReferences(ss)
              val p = generatePlanFromOnionSet0(ss0, onionSet, PreserveOriginal)
              subselects += ((s, p, m))
              false
            case _ => true
          }

          // return value is:
          // ( expr to replace in e -> ( replacement expr, seq( projections needed ) ) )
          def mkOptimizations(e: SqlExpr, curRewriteCtx: RewriteContext):
            Map[SqlExpr, (SqlExpr, Seq[(SqlExpr, SqlProj, OnionType, Boolean)])] = {

            val ret = new HashMap[SqlExpr, (SqlExpr, Seq[(SqlExpr, SqlProj, OnionType, Boolean)])]

            def handleBinopSpecialCase(op: Binop): Boolean = {
              val a = mkOptimizations(op.lhs, curRewriteCtx.restrictTo(Onions.ALL))
              val b = mkOptimizations(op.rhs, curRewriteCtx.restrictTo(Onions.ALL))
              a.get(op.lhs) match {
                case Some((aexpr, aprojs)) =>
                  b.get(op.rhs) match {
                    case Some((bexpr, bprojs)) =>
                      ret += (op -> (op.copyWithChildren(aexpr, bexpr), aprojs ++ bprojs))
                      false
                    case _ => true
                  }
                case _ => true
              }
            }

            // takes s and translates it into a server hom_agg expr, plus a
            // post-hom_agg-decrypt local sql projection (which extracts the individual
            // expression from the group)
            def handleHomSumSpecialCase(s: Sum) = {

              def pickOne(hd: Seq[HomDesc]): HomDesc = {
                assert(!hd.isEmpty)
                //println("hd: " + hd)
                // need to pick which homdesc to use, based on given analysis preference
                val m = hd.map(_.group).toSet
                assert( hd.map(_.table).toSet.size == 1 )
                val useIdx =
                  analysis.homGroupPreferences.get(hd.head.table).flatMap { prefs =>
                    prefs.foldLeft( None : Option[Int] ) {
                      case (acc, elem) =>
                        acc.orElse(if (m.contains(elem)) Some(elem) else None)
                    }
                  }.getOrElse(0)
                hd.filter(_.group == useIdx).head
              }

              def findCommonHomDesc(hds: Seq[Seq[HomDesc]]): Seq[HomDesc] = {
                assert(!hds.isEmpty)
                hds.tail.foldLeft( hds.head.toSet ) {
                  case (acc, elem) if !elem.isEmpty => acc & elem.toSet
                  case (acc, _)                     => acc
                }.toSeq
              }

              def translateForUniqueHomID(e: SqlExpr, aggContext: Boolean): Option[(SqlExpr, Seq[HomDesc])] = {
                def procCaseExprCase(c: CaseExprCase): Option[(CaseExprCase, Seq[HomDesc])] = {
                  CollectionUtils.optAnd2(
                    doTransformServer(c.cond, RewriteContext(Seq(Onions.PLAIN), aggContext, true)),
                    translateForUniqueHomID(c.expr, aggContext)).map {
                      case ((l, _), (r, hd)) => (CaseExprCase(l, r), hd)
                    }
                }
                e match {
                  case Sum(f, _, _) if aggContext =>
                    translateForUniqueHomID(f, false).map {
                      case (f0, hd) if !hd.isEmpty =>
                        val hd0 = pickOne(hd)
                        (AggCall("hom_agg", Seq(f0, StringLiteral(hd0.table), IntLiteral(hd0.group))), Seq(hd0))
                    }

                  case CaseWhenExpr(cases, Some(d), _) =>
                    CollectionUtils.optAnd2(
                      CollectionUtils.optSeq(cases.map(procCaseExprCase)),
                      translateForUniqueHomID(d, aggContext)).flatMap {
                        case (cases0, (d0, hd)) =>
                          // check that all hds are not empty
                          val hds = cases0.map(_._2) ++ Seq(hd)
                          // should have at least one non-empty
                          assert(!hds.filterNot(_.isEmpty).isEmpty)
                          val inCommon = findCommonHomDesc(hds)
                          if (inCommon.isEmpty) None else {
                            Some((CaseWhenExpr(cases0.map(_._1), Some(d0)), inCommon))
                          }
                      }

                  case CaseWhenExpr(cases, None, _) =>
                    CollectionUtils.optSeq(cases.map(procCaseExprCase)).flatMap {
                      case cases0 =>
                        // check that all hds are not empty
                        val hds = cases0.map(_._2)
                        // should have at least one non-empty
                        assert(!hds.filterNot(_.isEmpty).isEmpty)
                        val inCommon = findCommonHomDesc(hds)
                        if (inCommon.isEmpty) None else {
                          Some((CaseWhenExpr(cases0.map(_._1), None), inCommon))
                        }
                    }

                  case e: SqlExpr =>
                    getSupportedHOMRowDescExpr(e, analysis.subrels)
                  case _ => None
                }
              }

              translateForUniqueHomID(s, true).map {
                case (expr, hds) =>
                  assert(hds.size == 1)
                  val id0 = _hiddenNames.uniqueId()
                  val expr0 = FieldIdent(None, id0)
                  val projs =
                    Seq((expr0, ExprProj(expr, None),
                         HomGroupOnion(hds(0).table, hds(0).group), false))
                  (FunctionCall("hom_get_pos", Seq(expr0, IntLiteral(hds(0).pos))), projs)
              }
            }

            topDownTraverseContext(e, e.ctx) {
              case avg @ Avg(f, d, _) if curRewriteCtx.aggContext =>
                handleHomSumSpecialCase(Sum(f, d)).map { case (expr, projs) =>
                  val cnt = FieldIdent(None, _hiddenNames.uniqueId())
                  ret +=
                    (avg ->
                     (Div(expr, cnt), projs ++ Seq((cnt, ExprProj(CountStar(), None),
                      PlainOnion, false))))
                  false
                }.getOrElse(true)

              // TODO: do something about distinct
              case s: Sum if curRewriteCtx.aggContext =>
                //println("found sum s: " + s.sql)
                handleHomSumSpecialCase(s).map { value =>
                    ret += (s -> value)
                    false
                }.getOrElse(true)

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

          def mkProjections(e: SqlExpr): Seq[(SqlExpr, SqlProj, OnionType, Boolean)] = {
            val fields = resolveAliases(e).gatherFields
            def translateField(fi: FieldIdent, aggContext: Boolean) = {
              getSupportedExprConstraintAware(
                fi, Onions.DET | Onions.OPE, analysis.subrels,
                analysis.groupKeys, aggContext, None)
              .getOrElse {
                println("could not find DET/OPE enc for expr: " + fi)
                println("orig: " + e.sql)
                println("subrels: " + analysis.subrels)
                throw new RuntimeException("should not happen")
              }
            }

            fields.map {
              case (f, inAggCtx) =>
                if (inAggCtx && curRewriteCtx.respectStructure) {
                  val (ft, o) = translateField(f, true)
                  (f, ExprProj(GroupConcat(ft, ",", f.getType.tpe.isStringType), None), o, true)
                } else {
                  val (ft, o) = translateField(f, false)
                  (f, ExprProj(ft, None), o, false)
                }
            }
          }

          // if we are forcing computation on the client, omit optimizations for now
          // b/c it gets messy (and is probably not going to be correct)
          val opts:
            // need to be explicit about type here, otherwise scalac does not
            // handle properly
            Map[SqlExpr, (SqlExpr, Seq[(SqlExpr, SqlProj, OnionType, Boolean)])]
            = if (serverRewriteMode == ServerAll) mkOptimizations(e, curRewriteCtx)
              else Map.empty

          val e0ForProj = topDownTransformContext(e, e.ctx) {
            // replace w/ something dumb, so we can gather fields w/o worrying about
            // overlapping with what we already optimized away
            case e: SqlExpr if opts.contains(e) => replaceWith(IntLiteral(1))
            case _ => keepGoing
          }.asInstanceOf[SqlExpr]

          val e0 = topDownTransformContext(e, e.ctx) {
            case e: SqlExpr if opts.contains(e) => replaceWith(opts(e)._1)
            case _ => keepGoing
          }.asInstanceOf[SqlExpr]

          ClientComputation(
            e0,
            e,
            opts.values.flatMap(_._2).toSeq ++ mkProjections(e0ForProj),
            subselects.map(_._3).flatMap(x => x.map(_._2).flatMap(mkProjections)).toSeq,
            subselects.toSeq)
        }

        serverRewriteMode match {
          case ServerAll =>
            doTransformServer(e, curRewriteCtx).map(Left(_))
              .getOrElse(Right(doTransformClient(e, curRewriteCtx)))
          case ServerProj =>
            rewriteExprNonRecursive(e, curRewriteCtx).map(Left(_))
              .getOrElse(Right(doTransformClient(e, curRewriteCtx)))
          case ServerNone =>
            Right(doTransformClient(e, curRewriteCtx))
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
           if (sonions.size == 1) sonions.head else PlainOnion) )
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
                       if (sonions.size == 1) sonions.head else PlainOnion))),
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

    // --- subqueries --- //

    val subqueryRelations =
      cur.relations.map(_.flatMap(findSubqueryRelations)).getOrElse(Seq.empty)

    val subqueryRelationPlansWithOrigSS =
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
                  case FieldIdent(_, _, ColumnSymbol(relation, name, ctx, _), _) =>
                    if (ctx.relations(relation).isInstanceOf[SubqueryRelation]) {
                      val sr = ctx.relations(relation).asInstanceOf[SubqueryRelation]
                      val projExpr = sr.stmt.ctx.lookupProjection(name)
                      assert(projExpr.isDefined)
                      findOnionableExpr(projExpr.get).foreach { case (_, t, x) =>
                        def doSet = {
                          val idx = sr.stmt.ctx.lookupNamedProjectionIndex(name)
                          assert(idx.isDefined)
                          encVec( idx.get ) |= o
                        }

                        if (o == Onions.HOM_ROW_DESC) {
                          if (!onionSet.lookupPackedHOM(t, x).isEmpty) doSet
                        } else {
                          onionSet.lookup(t, x).filter(y => (y._2 & o) != 0).foreach { _ => doSet }
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
          val SelectStmt(p, r, f, g, o, _, ctx) = stmt

          def processRelation(r: SqlRelation) =
            r match {
              //case SubqueryRelationAST(subq, _, _) => buildForSelectStmt(subq)
              case JoinRelation(l, r, _, c, _)     =>
                traverseContext(c, ctx, Onions.PLAIN, buildForSelectStmt)
              case _                               => Seq.empty
            }

          p.foreach(e => traverseContext(e, ctx, Onions.ALL,              buildForSelectStmt))
          r.foreach(_.foreach(processRelation))
          f.foreach(e => traverseContext(e, ctx, Onions.PLAIN,            buildForSelectStmt))
          g.foreach(e => traverseContext(e, ctx, Onions.Comparable,       buildForSelectStmt))
          o.foreach(e => traverseContext(e, ctx, Onions.IEqualComparable, buildForSelectStmt))
        }

        // TODO: not the most efficient implementation
        buildForSelectStmt(cur)

        def checkCanExposePlain(e: SqlExpr) =
          e match {
            case _: CountStar => true
            case _: CountExpr => true
            case _ => false
          }

        (subq.alias,
         (generatePlanFromOnionSet0(
           subq.subquery,
           onionSet,
           EncProj(
             encVec.zip(subq.subquery.ctx.projections).map {
                // TODO: wildcard projections
               case (x, NamedProjection(_, e, _)) =>
                 if (x != 0) x else {
                   if (checkCanExposePlain(e)) {
                     (Onions.PLAIN | Onions.DET | Onions.OPE)
                   } else {
                     (Onions.DET | Onions.OPE)
                   }
                 }
             }.toSeq,
             true)),
          subq.subquery))
      }.toMap

    val subqueryRelationPlans = subqueryRelationPlansWithOrigSS.map {
      case (k, (v, _)) => (k, v)
    }.toMap

    // --- gather hom choices, and greedily pick the smallest --- //
    // --- covering set --- //

    val homGroupChoices = new ArrayBuffer[Seq[HomDesc]]

    def gatherHomRowDesc(e: SqlExpr) = {
      topDownTraverseContext(e, e.ctx) {
        case e: SqlExpr =>
          getSupportedHOMRowDescExpr(e, subqueryRelationPlans).map { case (_, hds) =>
            homGroupChoices += hds
            false
          }.getOrElse(true)
        case _ => true
      }
    }

    topDownTraverseContext(cur, cur.ctx) {
      case Sum(f, _, _) =>
        gatherHomRowDesc(f)
        false
      case Avg(f, _, _) =>
        gatherHomRowDesc(f)
        false
      case _ => true
    }

    //println("onionSet: " + onionSet)
    //println("hom groups:")
    //onionSet.getHomGroups.foreach {
    //  case (k, v) =>
    //    println("  " + k + ":")
    //    v.foreach(x => println("    " + x.map(_.sql).mkString(", ")))
    //}
    //println("homGroupChoices: " + homGroupChoices)
    //println("prefs: " + buildHomGroupPreference(homGroupChoices.toSeq))

    val analysis =
      RewriteAnalysisContext(subqueryRelationPlans,
                             buildHomGroupPreference(homGroupChoices.toSeq),
                             Map.empty)

    // --- relations --- //

    var remoteMaterializeAdded = false
    cur = cur.relations.map { r =>
      var mode: ServerRewriteMode = ServerAll
      def rewriteSqlRelation(s: SqlRelation): SqlRelation =
        s match {
          case t @ TableRelationAST(name, a, _) =>
            TableRelationAST(encTblName(name), a)
          case j @ JoinRelation(l, r, tpe, e, _)  =>
            rewriteExprForServer(e, RewriteContext(Seq(Onions.PLAIN), false, true),
                                 analysis, mode) match {
              case Left((e0, onion)) =>
                assert(onion == PlainOnion)
                assert(mode != ServerNone)
                j.copy(left = rewriteSqlRelation(l),
                       right = rewriteSqlRelation(r),
                       clause = e0).copyWithContext(null)
              case Right((optExpr, comp)) =>
                def traverseForConcreteRelations(s: SqlRelation): Seq[SqlRelation] =
                  s match {
                    case _: TableRelationAST         => Seq(s)
                    case _: SubqueryRelationAST      => Seq(s)
                    case JoinRelation(l, r, _, _, _) =>
                      traverseForConcreteRelations(l) ++
                      traverseForConcreteRelations(r)
                  }
                def extractNameFromConcreteRelation(s: SqlRelation): String =
                  s match {
                    case TableRelationAST(name, alias, _) => alias.getOrElse(name)
                    case SubqueryRelationAST(_, alias, _) => alias
                    case _ => throw new RuntimeException("non concrete relation")
                  }
                assert(optExpr.isEmpty || mode != ServerNone)
                // need to recurse first, to process children first
                mode = ServerNone
                val l0 = rewriteSqlRelation(l)
                val r0 = rewriteSqlRelation(r)
                newLocalJoinFilters += ((comp, (tpe match {
                    case LeftJoin  => traverseForConcreteRelations(r)
                    case RightJoin => traverseForConcreteRelations(l)
                    case InnerJoin => Seq.empty
                  }).map(extractNameFromConcreteRelation).toSet, j))
                j.copy(left = l0, right = r0,
                       clause = optExpr.map(_._1).getOrElse( Eq(IntLiteral(1), IntLiteral(1)) ))
                 .copyWithContext(null)
            }
          case r @ SubqueryRelationAST(_, name, _) =>
            subqueryRelationPlansWithOrigSS(name) match {
              case (p : RemoteSql, _) =>
                // if remote sql, then keep the subquery as subquery in the server sql,
                // while adding the plan's subquery children to our children directly
                mergeRemoteSql(p)
                SubqueryRelationAST(p.stmt, name)
              case (p, ss) =>
                // otherwise, add a RemoteMaterialize node
                val name0 = subRelnGen.uniqueId()
                finalSubqueryRelationPlans += ((RemoteMaterialize(name0, p), ss))
                remoteMaterializeAdded = true
                TableRelationAST(name0, Some(name))
            }
        }
      cur.copy(relations = Some(r.map(rewriteSqlRelation)))
    }.getOrElse(cur)

    // Special case optimization: This is kind of a hack
    // the idea is, if we have queries of the form:
    //
    //   SELECT ...
    //   FROM ( <inner subquery> ) AS ident
    //   WHERE ...
    //   GROUP BY ...
    //   ORDER BY ...
    //   LIMIT ...
    //
    // and we are forced to handle the subquery separately, instead
    // of doing a remote materialize, we should simply do all the operations
    // locally. We restrict this optimization to only apply when we have
    // one subquery

    if (encContext == PreserveOriginal && /* we only do this for the outer most query */
        remoteMaterializeAdded) { /* necessary but not sufficient condition for this opt to be applicable */
      // need to check if the query only had one relation, and that it was replaced with
      // a remote materialize plan
      cur.relations.filter(_.size == 1).flatMap(_.head match {
            case TableRelationAST(name0, _, _) =>
              finalSubqueryRelationPlans.filter(_._1.name == name0).headOption
            case _ => None
      }) match {
        case Some((rm, origSS)) =>
          // now that we have the RM, unwrap it and make sure we are able to see
          // all the subquery projections in the plaintext

          def unwrap(n: PlanNode): PlanNode = n match {
            case RemoteMaterialize(_, p) => unwrap(p)
            case LocalEncrypt(_, p)      => unwrap(p)
            case p                       => p
          }

          val p = unwrap(rm)
          val td = p.tupleDesc
          if (td.size == origSS.projections.size &&
              td.filter(_.onion != PlainOnion).isEmpty) {
            // TODO: not 100% sure this guarantees us that we have
            // exactly the original query in the clear, but it seems to be
            // a reasonable check

            // now we just translate each of the operations as local ops, re-writting
            // all references to the subquery in terms of tuple positions

            def rewritePre[N <: Node](n: Node): N = {
              topDownTransformation(n) {
                case FieldIdent(_, _, ColumnSymbol(_, col, _, _), _) =>
                  val p = origSS.ctx.lookupNamedProjectionIndex(col).get
                  (Some(TuplePosition(p)), false)

                case FieldIdent(_, _, ProjectionSymbol(name, ctx, _), _) =>
                  throw new RuntimeException("should not see proj symbol in pre-projection")

                case e => (None, true)
              }.asInstanceOf[N]
            }

            // where clause
            var res =
              cur.filter.map(f => LocalFilter(rewritePre[SqlExpr](f), f, p, Seq.empty)).getOrElse(p)

            // group by clause
            res =
              cur.groupBy.map(gb =>
                LocalGroupBy(gb.keys.map(x => rewritePre[SqlExpr](x)), gb.keys,
                             gb.having.map(x => rewritePre[SqlExpr](x)), gb.having,
                             res, Seq.empty)).getOrElse(res)

            // projections
            val trfms = cur.projections.map {
                case ExprProj(e, a, _) => (e, a, rewritePre(e))
                case StarProj(_)       => throw new RuntimeException("Unhandled")
              }

            res = LocalTransform(
              trfms.map(Right(_)), origSS.copy(orderBy = None, limit = None), res)

            // order by
            res =
              cur.orderBy.map { ob =>
                LocalOrderBy(
                  ob.keys.map {
                    case (FieldIdent(_, _, ColumnSymbol(_, col, _, _), _), ot) =>
                      (origSS.ctx.lookupNamedProjectionIndex(col).get, ot)

                    case (FieldIdent(_, _, ProjectionSymbol(name, ctx, _), _), ot) =>
                      (ctx.lookupNamedProjectionIndex(name).get, ot)

                    case (e, ot) => throw new RuntimeException("cannot handle: " + e)
                  }, res) }.getOrElse(res)

            // limit
            res = cur.limit.map(l => LocalLimit(l, res)).getOrElse(res)

            return verifyPlanNode(res)
          }

        case _ =>
      }
    }

    // --- filters --- //
    cur = cur
      .filter
      .map { x =>
        val mode = if (checkCanDoFilterOnServer) ServerAll else ServerProj
        rewriteExprForServer(x, RewriteContext(Seq(Onions.PLAIN), false, true),
                             analysis, mode)
      }.map {
        case Left((expr, onion)) =>
          assert(checkCanDoFilterOnServer)
          assert(onion == PlainOnion)
          cur.copy(filter = Some(expr))

        case Right((optExpr, comp)) =>
          // we defer the conversion of projections into group_concat until later,
          // after we know if the group by clause has gone through
          newLocalFilters += comp

          optExpr.map { case (expr, onion) =>
            assert(onion == PlainOnion)
            cur.copy(filter = Some(expr)) }.getOrElse(cur.copy(filter = None))
      }.getOrElse(cur)

    // --- group by --- //

    // this map is so when we do projections, we can know which can be projected
    // w/o having to wrap in a group_concat()
    val groupKeys = new HashMap[Symbol, (FieldIdent, OnionType)]
    cur = {
      cur.copy(groupBy = cur.groupBy.flatMap { gb =>

        // check if we can answer the keys
        val serverKeys = gb.keys.map(k =>
          getSupportedExpr(k, Onions.OPE, subqueryRelationPlans, None)
            .orElse(getSupportedExpr(k, Onions.DET, subqueryRelationPlans, None))
              .orElse(getSupportedExpr(k, Onions.PLAIN, subqueryRelationPlans, None)))

        // TODO: we must check, that if this group by has a having clause,
        // and we pulled a clause out of the where clause, then we cannot run
        // the having clause on the server (must apply a LocalGroupFilter)
        // we currently get this wrong
        if (checkCanDoGroupByOnServer &&
            serverKeys.flatten.size == serverKeys.size) {
          // ok to go ahead with server side group by

          val newHaving =
            gb.having.flatMap { x =>
              rewriteExprForServer(
                x,
                RewriteContext(Seq(Onions.PLAIN), true, true),
                analysis,
                ServerAll) match {

                case Left((expr, onion)) =>
                  assert(onion == PlainOnion)
                  Some(expr)
                case Right((optExpr, comp)) =>
                  newLocalGroupByHaving += comp
                  optExpr.map(_._1)
              }
            }

          gb.keys.zip(serverKeys.flatten).foreach {
            case (FieldIdent(_, _, sym, _), (fi: FieldIdent, o)) =>
              assert(sym ne null)
              groupKeys += ((sym -> (fi, o)))
            case _ =>
          }

          Some(gb.copy(keys = serverKeys.flatten.map(_._1), having = newHaving))

        } else {

          // force client side execution
          gb.having.foreach { x =>
            rewriteExprForServer(
              x, RewriteContext(Seq(Onions.PLAIN), true, false), analysis, ServerNone)
            match {
              case Left(_) => assert(false)
              case Right((optExpr, comp)) =>
                assert(optExpr.isEmpty)
                newLocalGroupByHaving += comp
            }
          }

          gb.keys.foreach { k =>
            rewriteExprForServer(
              k, RewriteContext(Seq(Onions.PLAIN), true, false), analysis, ServerNone)
            match {
              case Left(_) => assert(false)
              case Right((optExpr, comp)) =>
                assert(optExpr.isEmpty)
                newLocalGroupBy += comp
            }
          }

          None
        }
      })
    }

    // fix up filter projections to be group_concat()-ed if necessary
    if (stmt.projectionsInAggContext &&
        (!stmt.groupBy.isDefined || cur.groupBy.isDefined)) {

      // if we were originally in agg context, and we still haven't
      // removed a group by even after processing the group by, then
      // we need to do all filter projections in agg context (
      // unless we happen to project one of the group by keys)

      def fixupProjs(ccs: ArrayBuffer[ClientComputation]) = {
        ccs.map {
          case cc @ ClientComputation(_, _, p, sp, _) =>
            def procProjs(p: Seq[(SqlExpr, SqlProj, OnionType, Boolean)]) = {
              p.map {
                case t @ (o, ep @ ExprProj(e, _, _), _, v) if !v =>
                  def wrapWithGroupConcat(e: SqlExpr, h: Boolean) = GroupConcat(e, ",", h)
                  e match {
                    case FieldIdent(_, _, sym, _) =>
                      groupKeys.get(sym).map { _ => t }.getOrElse {
                        t.copy(_2 = ep.copy(expr = wrapWithGroupConcat(e, o.getType.tpe.isStringType)),
                               _4 = true)
                      }
                    case _ =>
                      t.copy(_2 = ep.copy(expr = wrapWithGroupConcat(e, o.getType.tpe.isStringType)),
                             _4 = true)
                  }
                case e => e
              }
            }
            cc.copy(projections = procProjs(p), subqueryProjections = procProjs(sp))
        }
      }

      val ccs0 =
        fixupProjs(newLocalJoinFilters.map(_._1)).zip(newLocalJoinFilters.map(x => (x._2, x._3)))
      newLocalJoinFilters.clear
      newLocalJoinFilters ++= ccs0.map { case (a, (b, c)) => (a, b, c) }

      val ccs1 = fixupProjs(newLocalFilters)
      newLocalFilters.clear
      newLocalFilters ++= ccs1

    }

    val analysis1 = analysis.copy(groupKeys = groupKeys.toMap)

    def checkGroupByExplicitlyRemoved: Boolean =
      cur.groupBy.isDefined && !newLocalGroupBy.isEmpty

    // order by
    cur = {
      def handleUnsupported(o: SqlOrderBy, groupByRemoved: Boolean) = {
        def getKeyInfoForExpr(f: SqlExpr) = {
          getSupportedExprConstraintAware(
            f, Onions.OPE,
            subqueryRelationPlans, groupKeys.toMap, true, None)
          .map { case (e, o) => (f, e, o) }
          .orElse {
            getSupportedExprConstraintAware(
              f, Onions.DET,
              subqueryRelationPlans, groupKeys.toMap, true, None)
            .map { case (e, o) => (f, e, o) }
          }
        }
        def mkClientCompFromKeyInfo(f: SqlExpr, fi: SqlExpr, o: OnionType) = {
          ClientComputation(f, f, Seq((f, ExprProj(fi, None), o, false)), Seq.empty, Seq.empty)
        }
        def searchProjIndex(e: SqlExpr): Option[Int] = {
          if (!e.ctx.projections.filter {
                case WildcardProjection => true
                case _ => false }.isEmpty) {
            // for now, if wildcard projection, don't do this optimization
            return None
          }
          e match {
            case FieldIdent(_, _, ProjectionSymbol(name, _, _), _) =>
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
        newLocalOrderBy ++= (
          o.keys.map { case (k, _) =>
            searchProjIndex(k).map(idx => Left(idx)).getOrElse {
              getKeyInfoForExpr(k)
                .map { case (f, fi, o) => Right(mkClientCompFromKeyInfo(f, fi, o)) }
                .getOrElse {
                  val mode = if (groupByRemoved) ServerNone else ServerAll
                  // TODO: why do we need to resolveAliases() here??
                  rewriteExprForServer(
                    resolveAliases(k),
                    RewriteContext(Seq(Onions.OPE), true, !groupByRemoved),
                    analysis1, mode) match {

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
        if (checkCanDoOrderByOnServer) {
          val mapped =
            o.keys.map(f =>
              (getSupportedExprConstraintAware(
                f._1, Onions.OPE, subqueryRelationPlans,
                groupKeys.toMap, true, None).map(_._1), f._2))
          if (mapped.map(_._1).flatten.size == mapped.size) {
            // can support server side order by
            Some(SqlOrderBy(mapped.map(f => (f._1.get, f._2))))
          } else {
            handleUnsupported(o, false)
          }
        } else {
          // check for case that a group by was *explicitly* removed
          handleUnsupported(o, checkGroupByExplicitlyRemoved)
        }
      })
      cur.copy(orderBy = newOrderBy)
    }

    // limit
    cur = cur.copy(limit = cur.limit.flatMap(l => {
      if (checkCanDoLimitOnServer) {
        Some(l)
      } else {
        newLocalLimit = Some(l)
        None
      }
    }))

    // projections
    cur = {

      val projectionCache = new HashMap[(SqlExpr, OnionType), (Int, Boolean)]
      def projectionInsert(
          origExpr: SqlExpr,
          origProjName: Option[String],
          p: SqlProj,
          o: OnionType,
          v: Boolean): Int = {

        assert(p.isInstanceOf[ExprProj])
        val ExprProj(e, _, _) = p
        val (i0, v0) =
          projectionCache.get((e.copyWithContext(null).asInstanceOf[SqlExpr], o))
          .getOrElse {
            // doesn't exist, need to insert
            val i = finalProjs.size
            finalProjs += ((origExpr, origProjName, p, o, v))

            // insert into cache
            projectionCache += ((e, o) -> (i, v))

            (i, v)
          }
        assert(v == v0)
        i0
      }

      def processClientComputation(comp: ClientComputation): CompProjMapping = {
        def proc(ps: Seq[(SqlExpr, SqlProj, OnionType, Boolean)]) =
          ps.map { case (e, p, o, v) =>
            (p, projectionInsert(e, None, p, o, v))
          }.toMap
        CompProjMapping(proc(comp.projections), proc(comp.subqueryProjections))
      }

      def procRegPair(comps: Seq[ClientComputation],
                      mappings: ArrayBuffer[CompProjMapping]) = {
        comps.foreach { c => mappings += processClientComputation(c) }
      }

      procRegPair(newLocalJoinFilters.map(_._1).toSeq, localJoinFilterPosMaps)
      procRegPair(newLocalFilters.toSeq, localFilterPosMaps)
      procRegPair(newLocalGroupBy.toSeq, localGroupByPosMaps)
      procRegPair(newLocalGroupByHaving.toSeq, localGroupByHavingPosMaps)

      newLocalOrderBy.foreach {
        case Left(_)  => localOrderByPosMaps += CompProjMapping.empty
        case Right(c) => localOrderByPosMaps += processClientComputation(c)
      }

      if (encContext.needsProjections) {
        cur.projections.zipWithIndex.foreach {
          case (ExprProj(e, a, _), idx) =>
            val onions = encContext match {
              case EncProj(o, r) =>
                if (r) Onions.toSeq(o(idx)) else Onions.completeSeqWithPreference(o(idx))
              case _             =>
                Onions.toSeq(Onions.ALL)
            }
            // TODO: is too conservative?
            val canDoServer =
              checkJoinClausesOnServer &&
              checkFilterClauseOnServer &&
              checkGroupByOnServer

            //println("--------")
            //println("onions: " + onions)
            //println("checkGroupByExplicitlyRemoved: " + checkGroupByExplicitlyRemoved)
            //println("canDoServer: " + canDoServer)
            //println(" -- checkJoinClausesOnServer: " + checkJoinClausesOnServer)
            //println(" -- checkFilterClauseOnServer: " + checkFilterClauseOnServer)
            //println(" -- checkGroupByOnServer: " + checkGroupByOnServer)
            //println("e: " + e.sql)

            rewriteExprForServer(
              e,
              RewriteContext(
                onions, true,
                checkCanDoFilterOnServer && !checkGroupByExplicitlyRemoved),
              analysis1,
              if (!canDoServer) ServerProj else ServerAll) match {

              case Left((expr, onion)) =>
                //println("orig e: " + e.sql)
                //println("got left: " + expr.sql )
                val stmtIdx = projectionInsert(e, a, ExprProj(expr, a), onion, false)
                projPosMaps += Left((stmtIdx, onion))
              case Right((optExpr, comp)) =>
                assert(!optExpr.isDefined)
                //println("got right: " + comp)
                val m = processClientComputation(comp)
                projPosMaps += Right((comp, m))
            }
          case (StarProj(_), _) => throw new RuntimeException("TODO: implement me")
        }
      }

      cur.copy(projections =
        finalProjs.map(_._3).toSeq ++
        (if (!finalProjs.isEmpty) Seq.empty else Seq(ExprProj(IntLiteral(1), None))))
    }

    def wrapDecryptionNodeSeq(p: PlanNode, m: Seq[Int]): PlanNode = {
      val td = p.tupleDesc
      val s = m.flatMap { pos => if (Onions.isDecryptable(td(pos).onion.onion)) Some(pos) else None }.toSeq
      if (s.isEmpty) p else LocalDecrypt(s, p)
    }

    def wrapDecryptionNodeMap(p: PlanNode, m: CompProjMapping): PlanNode =
      wrapDecryptionNodeSeq(p, m.projMap.values.toSeq)

    val tdesc =
      if (finalProjs.isEmpty) Seq(PosDesc(IntType(4), None, PlainOnion, false, false))
      else finalProjs.map {
        case (e, _, p, o, v) =>
          val tpe = e match {
            case FieldIdent(None, n, _, _) if n.startsWith(_hiddenNamePrefix) =>
              // for hidden names, use the projection expression
              val ExprProj(e, _, _) = p
              val r = e.getType
              //println(n + ": " + r)
              r
            case _ => e.findCanonical.getType
          }
          if (tpe.tpe == UnknownType) {
            println("ERROR: UnknownType for expr: " + e)
            println("e.findCanonical: " + e.findCanonical)
            println("p: " + p)
          }
          PosDesc(tpe.tpe, tpe.field.map(_.pos), o, tpe.field.map(_.partOfPK).getOrElse(false), v)
      }.toSeq

    // --join filters
    val stage0 =
      verifyPlanNode(
        newLocalJoinFilters
          .zip(localJoinFilterPosMaps)
          .foldLeft(
            RemoteSql(cur, tdesc,
                      finalSubqueryRelationPlans.toSeq,
                      subselectNodes.toMap) : PlanNode ) {
            case (acc, ((comp, rlnsToNull, reln), mapping)) =>
              if (rlnsToNull.isEmpty) {
                // regular inner join, use LocalFilter
                LocalFilter(comp.mkSqlExpr(mapping),
                            comp.origExpr,
                            wrapDecryptionNodeMap(acc, mapping),
                            comp.subqueries.map(_._2))
              } else {
                // outer join

                // map each projection (which should all be FieldIdents in this case)
                // to the relation it references

                val refs = finalProjs.map {
                  case (FieldIdent(_, _, ColumnSymbol(reln, _, _, _), _), _, _, _, _) => reln
                  case e =>
                    throw new RuntimeException("should see FieldIdent instead of " + e)
                }

                val toNullVec =
                  refs.zipWithIndex.flatMap { case (r, i) => if (rlnsToNull.contains(r)) Some(i) else None }

                LocalOuterJoinFilter(comp.mkSqlExpr(mapping),
                                     reln,
                                     toNullVec,
                                     wrapDecryptionNodeMap(acc, mapping),
                                     comp.subqueries.map(_._2))
              }
          })

    // --filters

    val stage1 =
      verifyPlanNode(
        newLocalFilters
          .zip(localFilterPosMaps)
          .foldLeft( stage0 : PlanNode ) {
            case (acc, (comp, mapping)) =>
              LocalFilter(comp.mkSqlExpr(mapping),
                          comp.origExpr,
                          wrapDecryptionNodeMap(acc, mapping),
                          comp.subqueries.map(_._2))
          })

    // --group bys

    val stage2 =
      verifyPlanNode(
        if (newLocalGroupBy.isEmpty) {
          newLocalGroupByHaving.zip(localGroupByHavingPosMaps).foldLeft( stage1 : PlanNode ) {
            case (acc, (comp, mapping)) =>
              LocalGroupFilter(comp.mkSqlExpr(mapping),
                               comp.origExpr,
                               wrapDecryptionNodeMap(acc, mapping),
                               comp.subqueries.map(_._2))
          }
        } else {
          assert(newLocalGroupBy.size == stmt.groupBy.get.keys.size)
          assert(newLocalGroupByHaving.size == 0 ||
                 newLocalGroupByHaving.size == 1)

          LocalGroupBy(
            newLocalGroupBy.zip(localGroupByPosMaps).map { case (c, m) => c.mkSqlExpr(m) },
            newLocalGroupBy.zip(localGroupByPosMaps).map { case (c, _) => c.origExpr },
            newLocalGroupByHaving.zip(localGroupByHavingPosMaps).headOption.map { case (c, m) => c.mkSqlExpr(m) },
            newLocalGroupByHaving.zip(localGroupByHavingPosMaps).headOption.map { case (c, _) => c.origExpr },
            wrapDecryptionNodeSeq(
              stage1,
              localGroupByPosMaps.flatMap(_.values) ++
                localGroupByHavingPosMaps.flatMap(_.values)),
            newLocalGroupByHaving.flatMap(_.subqueries.map(_._2)))
        })

    // --projections

    val stage3 =
      verifyPlanNode(
        if (encContext.needsProjections) {
          assert(!projPosMaps.isEmpty)

          val decryptionVec = (
            projPosMaps.flatMap {
              case Right((_, m)) => Some(m.values)
              case _             => None
            }.flatten ++ {
              projPosMaps.zipWithIndex.flatMap {
                case (Left((p, o)), idx) if !o.isPlain =>
                  encContext match {
                    case EncProj(onions, _) =>
                      // optimization: if onion is already in the correct
                      // form, no need to decrypt
                      if (onions(idx) != o.onion) Some(p) else None
                    case _ => Some(p)
                  }
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
              Right((comp.origExpr, None, comp.mkSqlExpr(mapping)))
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

          def isPrefixIdentityTransform(
            trfms: Seq[Either[Int, (SqlExpr, Option[String], SqlExpr)]]): Boolean = {

            trfms.zipWithIndex.foldLeft(true) {
              case (acc, (Left(p), idx)) => acc && p == idx
              case (acc, (Right(_), _))  => false
            }
          }

          // if trfms describes purely an identity transform, we can omit it
          val stage3 =
            if (trfms.size == s0.tupleDesc.size &&
                isPrefixIdentityTransform(trfms)) s0
            else LocalTransform(trfms, stmt.copy(orderBy = None, limit = None), s0)

          // need to update localOrderByPosMaps with new proj info
          val updateIdx = auxTrfmMSeq.toMap

          (0 until localOrderByPosMaps.size).foreach { i =>
            localOrderByPosMaps(i) = localOrderByPosMaps(i).update(updateIdx)
          }

          stage3
        } else {
          assert(projPosMaps.isEmpty)
          stage2
        })

    val stage4 =
      verifyPlanNode({
        if (!newLocalOrderBy.isEmpty) {
          assert(newLocalOrderBy.size == stmt.orderBy.get.keys.size)
          assert(newLocalOrderBy.size == localOrderByPosMaps.size)

          // do all the local computations to materialize keys

          val decryptionVec =
            newLocalOrderBy.zip(localOrderByPosMaps).flatMap {
              case (Right(ClientComputation(expr, _, proj, subProjs, sub)), m) =>
                if (proj.size == 1 && m.size == 1 &&
                    proj.head._3.isOneOf(Onions.OPE) &&
                    proj.head._1 == expr && sub.isEmpty) Seq.empty else m.values
              case _ => Seq.empty
            }

          val orderTrfms =
            newLocalOrderBy.zip(localOrderByPosMaps).flatMap {
              case (Left(p), _)  => None
              case (Right(c), m) => Some(Right((c.origExpr, None, c.mkSqlExpr(m))))
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
              stmt.copy(limit = None),
              LocalOrderBy(
                orderByVec,
                LocalTransform(
                  allTrfms,
                  stmt.copy(orderBy = None, limit = None),
                  (if (!decryptionVec.isEmpty) LocalDecrypt(decryptionVec, stage3) else stage3))))
          }
        } else {
          stage3
        }
      })

    val stage5 =
      verifyPlanNode(
        newLocalLimit.map(l => LocalLimit(l, stage4)).getOrElse(stage4))

    verifyPlanNode(
      encContext match {
        case PreserveCardinality => stage5
        case PreserveOriginal    =>

          // make sure everything is decrypted now
          val decryptionVec = stage5.tupleDesc.map(_.onion).zipWithIndex.flatMap {
            case (oo, idx) if !oo.isPlain => Some(idx)
            case _                        => None
          }

          assert(decryptionVec.isEmpty)
          stage5

        case EncProj(o, require) =>
          assert(stage5.tupleDesc.size == o.size)

          // optimization: see if stage5 is one layer covering a usable plan
          val usuablePlan = stage5 match {
            case LocalDecrypt(_, child) =>
              assert(child.tupleDesc.size == o.size)
              if (child.tupleDesc.map(_.onion).zip(o).filterNot {
                    case (onion, y) => onion.isOneOf(y)
                  }.isEmpty) Some(child) else None
            case _ => None
          }

          // special case if stage5 is usuable
          val stage5td = stage5.tupleDesc

          if (usuablePlan.isDefined) {
            usuablePlan.get
          } else if (!require || (stage5td.map(_.onion).zip(o).filter {
                case (onion, y) => onion.isOneOf(y)
              }.size == stage5td.size)) {
            stage5
          } else {

            //println("o: " + o)
            //println("stage5td: " + stage5td)
            //println("stage5: " + stage5.pretty)

            val dec =
              stage5td.map(_.onion).zip(o).zipWithIndex.flatMap {
                case ((onion, y), i) =>
                  if (onion.isOneOf(y) || onion.isPlain) Seq.empty else Seq(i)
              }

            val enc =
              stage5td.map(_.onion).zip(o).zipWithIndex.flatMap {
                case ((onion, y), i) =>
                  if (onion.isOneOf(y)) Seq.empty
                  else Seq((i, OnionType.buildIndividual(Onions.pickOne(y))))
              }

            val first  = if (dec.isEmpty) stage5 else verifyPlanNode(LocalDecrypt(dec, stage5))
            val second = if (enc.isEmpty) first  else verifyPlanNode(LocalEncrypt(enc, first))
            second
          }
      })
  }

  // if we want to answer e all on the server with onion constraints given by,
  // return the set of non-literal expressions and the corresponding bitmask of
  // acceptable onions (that is, any of the onions given is sufficient for a
  // server side rewrite), such that pre-computation is minimal
  private def getPotentialCryptoOpts(e: SqlExpr, o: Int):
    Option[Seq[(SqlExpr, Int)]] = {
    val ret = getPotentialCryptoOpts0(e, o)
    //println("getPotentialCryptoOpts: e=(%s), ret=(%s)".format(e.toString, ret.toString))
    ret
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

      case s @ Sum(expr, _, _) if test(Onions.HOM_AGG) =>
        getPotentialCryptoOpts0(expr, Onions.HOM_ROW_DESC)

      case Avg(expr, _, _) if test(Onions.HOM_AGG) =>
        getPotentialCryptoOpts0(expr, Onions.HOM_ROW_DESC)

      case CountStar(_) => Some(Seq.empty)

      case CountExpr(expr, _, _) =>
        getPotentialCryptoOpts0(expr, Onions.DET)

      case CaseWhenExpr(cases, default, _) =>
        def procCaseExprCase(c: CaseExprCase, constraints: Int) = {
          CollectionUtils.optAnd2(
            getPotentialCryptoOpts0(c.cond, Onions.ALL),
            getPotentialCryptoOpts0(c.expr, constraints)).map { case (l, r) => l ++ r }
        }
        default match {
          case Some(d) =>
            CollectionUtils.optSeq(
              cases.map(c => procCaseExprCase(c, constraints)) ++
              Seq(getPotentialCryptoOpts0(d, constraints))).map(_.flatten)
          case None =>
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

      case _ : DependentFieldPlaceholder => Some(Seq.empty)

      case e : SqlExpr
        if containsNonPlain(constraints) && e.getPrecomputableRelation.isDefined =>
        Some(Seq((e, pickNonPlain(constraints))))

      case e => None
    }
  }

  type CandidatePlans = Seq[(PlanNode, EstimateContext)]

  private def mkGlobalPreCompExprMap(
    onionSets: Seq[OnionSet]): Map[String, Seq[SqlExpr]] = {
    type PreCompMap = Map[String, Map[SqlExpr, Int]]
    onionSets.map(_.getPrecomputedExpressions).foldLeft(
      Map.empty : PreCompMap ) {
      case (acc, elem) =>
        def merge(lhs: PreCompMap, rhs: PreCompMap): PreCompMap = {
          (lhs.keys ++ rhs.keys).map { k =>
            def merge(lhs: Map[SqlExpr, Int], rhs: Map[SqlExpr, Int]): Map[SqlExpr, Int] = {
              (lhs.keys ++ rhs.keys).map { e =>
                (e, lhs.getOrElse(e, 0) | rhs.getOrElse(e, 0))
              }.toMap
            }
            (k, merge(lhs.getOrElse(k, Map.empty), rhs.getOrElse(k, Map.empty)))
          }.toMap
        }
        merge(acc, elem)
    }.map { case (k, v) => (k, v.keys.toSeq) }
  }

  // return value 1-to-1 with input stmts (stmts.size == ret.size)
  def generateCandidatePlans(stmts: Seq[SelectStmt]): Seq[CandidatePlans] = {
    val onionSets0 /* Seq[Seq[OnionSet]] */ = stmts.map(generateOnionSets)

    //println(onionSets0)

    // ------- create a global set of precomputed expressions -------- //

    val precompExprMap = mkGlobalPreCompExprMap(onionSets0.flatten)

    //println("precompExprMap: " + precompExprMap)

    // make onionSets all reference global precomputed expressions
    // (HOM groups are still local though, b/c they are handled separately)
    val onionSets = onionSets0.zip(stmts).map { case (os, stmt) =>
      os.map(_.withGlobalPrecompExprs(precompExprMap)).map(c => fillOnionSet(stmt, c)).toSet.toSeq
    }

    // ------- generate various permutations -------- //

    val perms /* Seq[ Seq[Seq[(Set[String], OnionSet)]] ] */ =
      onionSets.map { os /* Seq[OnionSet] */ =>
        CollectionUtils.powerSetMinusEmpty(
          os.map(x => (x.groupsForRelations.toSet, x.withoutGroups)))
      }

    // the candidates don't have groups in them, but have
    // the set of relations for which it had created a HOM
    // group for

    val candidates /* :Seq[ Seq[(Seq[String], OnionSet)] ] */ =
      perms.map { p /* :Seq[Seq[(Set[String], OnionSet)]] */ =>
        val merged = p.map { g /* :Seq[(Set[String], OnionSet)] */ =>
          (g.flatMap(_._1).toSet.toSeq, OnionSet.mergeSeq(g.map(_._2)))
        }
        CollectionUtils.uniqueInOrder(merged)
      }

    // ------- create a global set of groups -------- //
    // the following block of code deals with taking individual groups
    // per query, and generating permutations of groups from all queries
    // (on a per-relation basis)

    // for each relation
    //   for each query
    //     build an global index with all the unique group expressions needed
    //     mapped back to the query
    //
    //     generate all possible group splits
    //
    //     greedily merge group splits which only cover one relation together
    //
    // for each query
    //   generate cross product of all groups for each relation needed
    //   in group context
    //
    //   cross the groups cross product with the onion power set
    //
    //   take the previous list + onion power set (w/o groups), and invoke
    //   generatePlanFromOnionSet() w/ each elem in the list

    val perQueryExprIndex = onionSets.map { os =>
      val maps = os.map(_.getHomGroups.map { case (k, v) =>
        (k, v.flatMap(_.toSet).toSet)
      })

      // merge maps accordingly
      maps.foldLeft( Map.empty : Map[String, Set[SqlExpr]] ) {
        case (acc, m) =>
          acc ++ m.map { case (k, v) =>
            (k, acc.get(k).getOrElse(Set.empty) ++ v)
          }
      }
    }

    //println("perQueryExprIndex:")
    //perQueryExprIndex.zipWithIndex.foreach { case (i, idx) =>
    //  println("q%d:".format(idx))
    //  i.foreach { case (k, v) =>
    //    println("  %s:".format(k))
    //    v.foreach(e => println("    %s".format(e.sql)))
    //  }
    //}

    // build global index, assigning a unique number (per relation)
    // to each unique expression amongst all queries
    val globalExprIndex = new HashMap[String, HashMap[SqlExpr, Int]]

    // Seq[Map[String, Set[Int]]]
    val perQueryInterestMap = perQueryExprIndex.map { m =>
      m.map { case (k, v) =>
        val relIdx = globalExprIndex.getOrElseUpdate(k, new HashMap[SqlExpr, Int])
        (k, v.map { expr => relIdx.getOrElseUpdate(expr, relIdx.size) }.toSet)
      }.toMap
    }

    println("globalExprIndex:")
    globalExprIndex.foreach { case (k, m) =>
      println("%s:".format(k))
      m.foreach { case (e, id) =>
        println("  %s : %d".format(e.sql, id))
      }
    }

    //println("perQueryInterestMap:")
    //println(perQueryInterestMap)

    // build a query index (mapping per relation ID to set of queries which use it)
    val queryReverseIndex /* Map[String, Map[Int, Set[Int]]] */ =
      perQueryInterestMap.zipWithIndex.foldLeft( Map.empty : Map[String, Map[Int, Set[Int]]] ) {
        case (acc, (m, idx)) =>
          acc ++ m.map { case (k, v) =>
            val accMap = acc.get(k).getOrElse(Map.empty)
            (k, accMap ++ v.map {
              exprId => (exprId, accMap.get(exprId).getOrElse(Set.empty) ++ Set(idx)) })
          }
      }

    //println("queryReverseIndex:")
    //println(queryReverseIndex)

    val globalExprSplits /* Map[String, Set[ Set[Set[Int]] ]] */ =
      globalExprIndex.map { case (k, v) =>
        assert(v.values.size == v.values.toSet.size)
        val exprs = v.values.toSet
        val allNonDupGroupings = CollectionUtils.allPossibleGroupings(exprs)

        val relnReverseIdx = queryReverseIndex(k)

        // ret contains the query id (if Some(_))
        def groupForOnlyOneQuery(s: Set[Int]): Option[Int] = {
          val interest = s.flatMap(relnReverseIdx)
          assert(!interest.isEmpty)
          if (interest.size == 1) Some(interest.head) else None
        }

        def allInterestedQueries(s: Set[Int]): Set[Int] = {
          s.flatMap(relnReverseIdx)
        }

        // give each query its own copy of the split
        // (unique amongst shared sets)
        val optimalSplits = perQueryInterestMap.flatMap { m => m.get(k) }.toSet

        // greedy merging for each group
        (k, allNonDupGroupings.map { grouping /* Set[Set[Int]] */ =>
          val gseq = grouping.toIndexedSeq
          val gmap = gseq.zipWithIndex.flatMap { case (g, i) =>
            groupForOnlyOneQuery(g).map(q => (q, i))
          }.groupBy(x => x._1).map { case (k, v) => (k, v.map(_._2))}.toMap

          //println("gseq: " + gseq)
          //println("gseq interest: " + gseq.map(allInterestedQueries))
          //println("gmap: " + gmap)

          val ungrouped = (0 until gseq.size).toSet -- gmap.values.flatten.toSet

          ungrouped.map(gseq(_)).toSet ++ gmap.values.map(ii => ii.flatMap(gseq(_)).toSet).toSet
        } ++ Set(optimalSplits))
      }

    //println("globalExprSplits:")
    //println(globalExprSplits)

    val revGlobalExprIndex = globalExprIndex.map { case (k, v) =>
      (k, v.map { case (k, v) => (v, k) }.toMap)
    }.toMap

    // TODO: parallelize this loop
    val uniquePlans =
      stmts.zip(candidates).zip(perQueryInterestMap).zipWithIndex.map {
        case (((stmt, cands), im), idx) if im.isEmpty =>

          println("trying query %d with %d onions...".format(idx, cands.size))
          val (time, res) =
            timedRunMillis(
              CollectionUtils.uniqueInOrderWithKey(
                cands.map(_._2).map { o =>
                  //println("  ...onion = " + o.compactToString)
                  (stmt, generatePlanFromOnionSet(stmt, o), o)
                })(_._2))
          println("  generated %d unique plans for query %d in %f ms".format(res.size, idx, time))
          res

        case (((stmt, cands), im), idx) =>

          if (im.keys.size > 1) {
            System.err.println("WARNING: query is generating multi-group candidates:")
            System.err.println("  " + stmt.sql)
            throw new RuntimeException("TODO: implement handling > 1 group relation")
          }

          val reln = im.keys.head
          val exprSplits /* Seq[ Seq[Seq[SqlExpr]] ] */ =
            globalExprSplits(reln).toSeq.map { split =>
              split.toSeq.map { group =>
                group.toSeq.map(i => revGlobalExprIndex(reln)(i))
              }
            }

          val oSets =
            for (split <- exprSplits; cand <- cands) yield {
              // TODO: relax assumption
              assert(cand._1.size <= 1)
              if (cand._1.contains(reln)) {
                cand._2.withGroups(Map(reln -> split))
              } else {
                cand._2
              }
            }

          println("trying query %d with %d onions...".format(idx, oSets.size))
          val (time, res) =
            timedRunMillis(
              CollectionUtils.uniqueInOrderWithKey(
                oSets.map {
                  o => (stmt, generatePlanFromOnionSet(stmt, o), o)
                })(_._2))
          println("  generated %d unique plans for query %d in %f ms".format(res.size, idx, time))
          res
      }

    val globalOpts = buildGlobalOptsFromPlans(uniquePlans.flatMap(_.map(x => (x._2, x._3))))
    uniquePlans.map(_.map(x => mkGloballyAwarePlanWithEstimate(x._1, x._3, x._2, globalOpts)))
  }

  @inline
  private def fillOnionSet(stmt: SelectStmt, o: OnionSet): OnionSet = {
    o.complete(stmt.ctx.defns)
  }

  @inline
  private def mkPowerSetOnionSet(os: Seq[OnionSet]): Seq[Seq[OnionSet]] = {
    println("powerset over %d elements...".format(os.size))
    val (time, res) = timedRunMillis(CollectionUtils.powerSetMinusEmpty(os))
    println("  took %f ms".format(time))
    res
  }

  private def mkGloballyAwarePlanWithEstimate(
    origPlan: SelectStmt,
    origOS: OnionSet,
    rewrittenPlan: PlanNode,
    globalOpts: GlobalOpts): (PlanNode, EstimateContext) = {

    // TODO: this is implemented very hackily right now- it works by simply
    // doing string matching on fields in the query plan. there's no
    // reason we have to implement it this way- it just takes a lot mindless
    // work in order to propagate the onion information from plan generation
    // correctly, and this approach currently works fine for TPC-H, so we
    // go w/ this for now and leave a more complete solution as future work

    import FieldNameHelpers._

    val requiredOnions = new HashMap[String, HashMap[String, Int]]
    val precomputed = new HashMap[String, HashMap[String, Int]]
    val homGroups = new HashMap[String, HashSet[Int]]

    val pnew = topDownTransformation(rewrittenPlan) {
      case r @ RemoteSql(stmt, _, _, _) =>
        val s = topDownTransformation(stmt) {
          case FieldIdent(qual, name, _, _) =>
            assert(qual.isDefined)
            val qual0 = basename(qual.get)
            val name0 = basename(name)
            assert(!name0.startsWith("virtual_local"))

            if (name0.startsWith("virtual_global")) {
              val m = precomputed.getOrElseUpdate(qual0, new HashMap[String, Int])
              m.put(name0, m.getOrElse(name0, 0) | encType(name).get)
            } else {
              val cols = origPlan.ctx.defns.lookupByColumnName(name0)
              if (cols.size > 1) {
                println("failure: cols: " + cols)
              }
              assert(cols.size <= 1) // no reason this has to hold, but assume for now b/c
                                     // it holds for TPC-H. really, we should be propagating
                                     // more information during plan generation phase - see comment
                                     // above
              cols.headOption.foreach { c =>
                encType(name).foreach { o =>
                  val m = requiredOnions.getOrElseUpdate(c._1, new HashMap[String, Int])
                  m.put(name0, m.getOrElse(name0, 0) | o)
                }
              }
            }

            keepGoing // no need for replacement

          case ag @ AggCall("hom_agg", Seq(a0, a1 @ StringLiteral(tbl, _), IntLiteral(grp, _)), _) =>
            // need to replace this local group id with a global group id
            // (the group id is local to the onion set used to generate the plan)

            val Some(gexprs) = origOS.lookupPackedHOMById(tbl, grp.toInt)

            // map local group expr set to global group id
            val ggidx = globalOpts.homGroups(tbl).indexWhere(_ == gexprs)
            assert(ggidx != -1) // must be valid

            homGroups.getOrElseUpdate(tbl, new HashSet[Int]) += ggidx

            replaceWith(ag.copy(args = Seq(a0, a1, IntLiteral(ggidx))))

          case _ => (None, true)
        }.asInstanceOf[SelectStmt]

        (Some(r.copy(stmt = s)), true)
      case _ => keepGoing
    }

    (pnew, EstimateContext(
      origPlan.ctx.defns,
      globalOpts,
      requiredOnions.map { case (k, v) => (k, v.toMap) }.toMap,
      precomputed.map { case (k, v) => (k, v.toMap) }.toMap,
      homGroups.map { case (k, v) => (k, v.toSet) }.toMap))
  }

  // input is seq( (plan, onionset used to build plan) )
  private def buildGlobalOptsFromPlans(
    plans: Seq[(PlanNode, OnionSet)]): GlobalOpts = {

    // here, we assume that all precomputed expressions have been
    // normalized, so each virtual name refers to a unique expression

    // each plan generates its version of global opts, then we
    // will merge the opts at the end
    def analyzePlan(plan: PlanNode, os: OnionSet): GlobalOpts = {
      import FieldNameHelpers._

      val precomputed = new HashMap[String, HashMap[String, SqlExpr]]
      val homGroups = new HashMap[String, HashSet[Seq[SqlExpr]]]

      topDownTraversal(plan) {
        case RemoteSql(stmt, _, _, _) =>
          topDownTraversal(stmt) {
            case FieldIdent(qual, name, _, _) =>
              assert(qual.isDefined)
              val qual0 = basename(qual.get)
              val name0 = basename(name)

              // all the local exprs should be replaced with a global name
              assert(!name0.startsWith("virtual_local"))

              if (name0.startsWith("virtual_global")) {
                os.lookupPrecomputedByName(qual0, name0).foreach {
                  case (expr, o) =>
                    val Some(o0) = encType(name)
                    assert((o & o0) != 0)
                    val m = precomputed.getOrElseUpdate(qual0, new HashMap[String, SqlExpr])
                    m.get(name0).foreach(e => assert(e == expr)) // b/c we canonicalized...
                    m.put(name0, expr)
                }
              }

              false

            case ag @ AggCall("hom_agg", Seq(_, StringLiteral(tbl, _), IntLiteral(grp, _)), _) =>
              val group = os.lookupPackedHOMById(tbl, grp.toInt)
              assert(group.isDefined)
              val s = homGroups.getOrElseUpdate(tbl, new HashSet[Seq[SqlExpr]])
              if (!s.contains(group.get)) s += group.get

              false

            case _ => true
          }
          true
        case _ => true
      }

      GlobalOpts(precomputed.map { case (k, v) => (k, v.toMap) }.toMap,
                 homGroups.map { case (k, v) => (k, v.toSeq) }.toMap)
    }

    plans.foldLeft(GlobalOpts.empty) {
      case (acc, (plan, os)) =>
        val ap = analyzePlan(plan, os)
        //println("ap: " + ap)
        acc.merge(ap)
    }
  }

  def generateCandidatePlans(stmt: SelectStmt): CandidatePlans = {
    val o0 = generateOnionSets(stmt)

    //println("onions:")
    //o0.foreach { o =>
    //  println(o)
    //  println()
    //}

    val precompExprMap = mkGlobalPreCompExprMap(o0)
    val o = o0.map(_.withGlobalPrecompExprs(precompExprMap)).map(c => fillOnionSet(stmt, c)).toSet.toSeq

    //println("onions to powerset:")
    //o.foreach { o =>
    //  println(o)
    //  println()
    //}

    val perms = mkPowerSetOnionSet(o)

    // merge all perms, then unique
    val candidates = CollectionUtils.uniqueInOrder(perms.map(p => OnionSet.mergeSeq(p)))

    //println("candidate onions")
    //candidates.foreach { o =>
    //  println(o.compactToString)
    //  println()
    //}

    println("trying query with %d onions...".format(candidates.size))
    if (candidates.size > 10000) {
      println("  WARNING: a high number of onions for sql: " + stmt.sql)
    }
    val (time, uniquePlans) =
      timedRunMillis(
        CollectionUtils.uniqueInOrderWithKey(
          candidates.map {
            o => (stmt, generatePlanFromOnionSet(stmt, o), o)
          })(_._2))
    println("  generated %d unique plans in %f ms".format(uniquePlans.size, time))

    // compute global opts from the unique plans
    val globalOpts = buildGlobalOptsFromPlans(uniquePlans.map(x => (x._2, x._3)))
    uniquePlans.map(x => mkGloballyAwarePlanWithEstimate(x._1, x._3, x._2, globalOpts))
  }

  def generateOnionSets(stmt: SelectStmt): Seq[OnionSet] = {

    def traverseContext(
      start: Node,
      ctx: Context,
      onion: Int,
      bootstrap: Seq[OnionSet],
      selectFn: (SelectStmt, Seq[OnionSet]) => Seq[OnionSet]): Seq[OnionSet] = {

      var workingSet : Seq[OnionSet] = bootstrap
      val subselectSets = new ArrayBuffer[OnionSet]

      def add(exprs: Seq[(SqlExpr, Int)]): Boolean = {
        val e0 = exprs.map { case (e, o) => findOnionableExpr(e).map(e0 => (e0, o)) }.flatten
        if (e0.size == exprs.size) {
          e0.foreach {
            case ((_, t, e), o) =>
              if (o == Onions.HOM_ROW_DESC) {
                workingSet.foreach(_.addPackedHOMToLastGroup(t, e))
              } else {
                workingSet.foreach(_.add(t, e, o))
              }
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
            // TODO: should we still traverse for subselects (as we do below)?

          case None =>

            // traverse for subselects
            topDownTraversal(e) {
              case Subselect(ss, _) =>
                subselectSets ++= selectFn(ss, Seq(new OnionSet))
                false // we'll recurse in the invocation to selectFn()
              case _ => true
            }

            e match {

              // one-level deep binop optimizations
              // TODO: why do we need this?
              case Plus(l, r, _)  => binopOp(l, r)
              case Minus(l, r, _) => binopOp(l, r)
              case Mult(l, r, _)  => binopOp(l, r)
              case Div(l, r, _)   => binopOp(l, r)

              case Gt(l, r, _)    => binopOp(l, r)
              case Ge(l, r, _)    => binopOp(l, r)
              case Lt(l, r, _)    => binopOp(l, r)
              case Le(l, r, _)    => binopOp(l, r)

              // TODO: more opts?
              case _              =>
            }
        }
      }

      def procExpr(e: SqlExpr, o: Int) = {
        val clauses = splitTopLevelClauses(e)
        assert(!clauses.isEmpty)
        if (clauses.size == 1) {
          procExprPrimitive(clauses.head, o)
        } else {
          workingSet =
            clauses.flatMap(c => traverseContext(c, ctx, Onions.PLAIN, workingSet.map(_.copy), selectFn))
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

      workingSet ++ subselectSets.toSeq
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
          case JoinRelation(l, r, _, c, _)     =>
            val c0 =
              traverseContext(c, ctx, Onions.PLAIN, bootstrap.map(_.copy), buildForSelectStmt)
            c0 ++ processRelation(l) ++ processRelation(r)
          case _                               => Seq.empty
        }
      val s1 = r.map(_.flatMap(processRelation))
      val s2 = f.map(e => traverseContext(e, ctx, Onions.PLAIN, bootstrap.map(_.copy), buildForSelectStmt))
      val s3 = g.map(e => traverseContext(e, ctx, Onions.Comparable, bootstrap.map(_.copy), buildForSelectStmt))
      val s4 = o.map(e => traverseContext(e, ctx, Onions.IEqualComparable, bootstrap.map(_.copy), buildForSelectStmt))

      //println("onions for stmt: " + stmt.sql)

      //println("bootstrap onions (size = %d):".format(bootstrap.size))
      //bootstrap.foreach(println)
      //println()

      //println("s0 onions (size = %d):".format(s0.size))
      //s0.foreach(println)
      //println()

      //println("s1 onions (size = %d):".format(s1.map(_.size).getOrElse(0)))
      //s1.foreach(_.foreach(println))
      //println()

      //println("s2 onions (size = %d):".format(s2.map(_.size).getOrElse(0)))
      //s2.foreach(_.foreach(println))
      //println()

      //println("s3 onions (size = %d):".format(s3.map(_.size).getOrElse(0)))
      //s3.foreach(_.foreach(println))
      //println()

      //println("s4 onions (size = %d):".format(s4.map(_.size).getOrElse(0)))
      //s4.foreach(_.foreach(println))
      //println()

      //println("-----")

      (s0 ++ s1.getOrElse(Seq.empty) ++ s2.getOrElse(Seq.empty) ++
      s3.getOrElse(Seq.empty) ++ s4.getOrElse(Seq.empty)).filterNot(_.isEmpty)
    }

    buildForSelectStmt(stmt, Seq(new OnionSet))
  }
}
