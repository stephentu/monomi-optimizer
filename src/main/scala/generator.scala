import scala.collection.mutable.ArrayBuffer

trait Generator extends Traversals with Transformers {

  private def findOnionableExpr(e: SqlExpr): Option[(String, SqlExpr)] = {
    val r = e.getPrecomputableRelation
    if (r.isDefined) Some((r.get, e)) 
    else e match {
      // TODO: this special case currently doesn't help you
      case FieldIdent(_, _, Symbol(relation, name, ctx), _) =>
        assert(ctx.relations(relation).isInstanceOf[SubqueryRelation])
        ctx
          .relations(relation)
          .asInstanceOf[SubqueryRelation]
          .stmt.ctx.lookupProjection(name).flatMap(findOnionableExpr)
      case _ => None
    }
  }

  def generatePlanFromOnionSet(stmt: SelectStmt, onionSet: OnionSet): PlanNode =
    generatePlanFromOnionSet0(stmt, onionSet, PreserveOriginal)


  abstract trait EncContext
  case object PreserveOriginal extends EncContext
  case object PreserveCardinality extends EncContext
  case class SingleEncProj(onion: Int) extends EncContext

  // if encContext is PreserveOriginal, then the plan node generated faithfully
  // recreates the original statement- that is, the result set has the same
  // (unencrypted) type as the result set of stmt.
  //
  // if encContext is PreserveCardinality, then this function is free to generate
  // plans which only preserve the *cardinality* of the original statement. the
  // result set, however, is potentially left encrypted. this is useful for generating
  // plans such as subqueries to EXISTS( ... ) calls
  // 
  // if encContext is SingleEncProj, then stmt is expected to have exactly one
  // projection (this is asserted)- and plan node is written so it returns the
  // single projection encrypted with the onion given by SingleEncProj
  private def generatePlanFromOnionSet0(
    stmt: SelectStmt, onionSet: OnionSet, encContext: EncContext): PlanNode = {

    encContext match { 
      case SingleEncProj(_) => assert(stmt.ctx.projections.size == 1) 
      case _ =>
    }

    // order is
    // 1) where clause (filter)
    // 2) group by 
    // 3) select clause (projections)
    // 4) order by

    // NOTE: assumes that all expressions are in disjunctive normal form (DNF)

    def getSupportedExpr(e: SqlExpr, o: Int): Option[FieldIdent] = {
      val e0 = findOnionableExpr(e)
      //println("e = " + e)
      //println("e0 = " + e0)
      e0.flatMap { x =>
        onionSet.lookup(x._1, x._2).filter(y => (y._2 & o) != 0).map {
          case (basename, _) =>
            FieldIdent(Some(x._1), basename + "_" + Onions.str(o)) 
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
        *     the encryption onion which is being projected from the server side query ) */
       projections: Seq[(SqlExpr, SqlProj, Int)], 

       /* additional subqueries needed for conjunction. the tuple is as follows:
        *   ( (original) subselect form conjunction which will be replaced with PlanNode,
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
        topDownTransformation(expr) {
          case s: Subselect =>
            (Some(SubqueryPosition(smap(s)._2)), false)

          case e: SqlExpr =>
            pmap.get(e).map { p => (Some(TuplePosition(mappings(p))), false) }.getOrElse(keepGoing)

          case _ => keepGoing

        }.asInstanceOf[SqlExpr]
      }
    }

    var cur = stmt // the current statement, as we update it

    val _generator = new NameGenerator("_projection")

    val newLocalFilters = new ArrayBuffer[ClientComputation]
    val localFilterPosMaps = new ArrayBuffer[Map[SqlProj, Int]]

    val newLocalGroupBy = new ArrayBuffer[ClientComputation]
    val localGroupByPosMaps = new ArrayBuffer[Map[SqlProj, Int]]

    val newLocalOrderBy = new ArrayBuffer[ClientComputation]
    val localOrderByPosMaps = new ArrayBuffer[Map[SqlProj, Int]]

    var newLocalLimit: Option[Int] = None

    // these correspond 1 to 1 with the original projections
    val projPosMaps = new ArrayBuffer[(Int, Int, Option[(ClientComputation, Map[SqlProj, Int])])]

    // these correspond 1 to 1 with the new projections in the encrypted
    // re-written query
    val finalProjs = new ArrayBuffer[(SqlProj, Int)]

    def answerWithoutModification(n: Node) = (Some(n.copyWithContext(null)), false)

    abstract trait RewriteContext {
      def aggsValid: Boolean = false
    }
    case object FilterCtx extends RewriteContext
    case object GroupByKeyCtx extends RewriteContext
    case object GroupByHavingCtx extends RewriteContext { override def aggsValid = true }
    case object OrderByKeyCtx extends RewriteContext
    case class ProjCtx(o: Int) extends RewriteContext { override def aggsValid = true }
    case class ProjUnaggCtx(o: Int) extends RewriteContext

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

      var conjunctions: Option[ClientComputation] = None

      def mergeConjunctions(that: ClientComputation) = {
        conjunctions match {
          case Some(thiz) => conjunctions = Some(thiz mergeConjunctions that)
          case None => conjunctions = Some(that)
        }
      }

      def mkOnionRetVal(o: Int) = rewriteCtx match {
        case FilterCtx | GroupByHavingCtx => 0
        case ProjCtx(_) | ProjUnaggCtx(_) => o
        case _ => throw new RuntimeException("TODO: impl")
      }

      val onionRetVal = new SetOnce[Int] // TODO: FIX THIS HACK

      val cannotAnswerExpr = (Some(IntLiteral(1)), false)

      def mkProjections(e: SqlExpr): Seq[(SqlExpr, SqlProj, Int)] = {
        // filter out all fields
        assert(e.canGatherFields)
        val fields = e.gatherFields
        def translateField(e: SqlExpr) = 
          CollectionUtils.optOrEither2(
            getSupportedExpr(e, Onions.DET),
            getSupportedExpr(e, Onions.OPE))
          .getOrElse(throw new Exception("should not happen")) match { 
            case Left(e) => (e, Onions.DET)
            case Right(e) => (e, Onions.OPE)
          }
        fields.map { 
          case (f, false) =>
            val (ft, o) = translateField(f)
            (f, ExprProj(ft, Some(_generator.uniqueId())), o)
          case (f, true) =>
            val (ft, o) = translateField(f)
            (f, ExprProj(GroupConcat(ft, ","), Some(_generator.uniqueId())), o)
        }
      }

      def cannotAnswer(e: SqlExpr): (Option[Node], Boolean) = {
        // TODO: rewrite query deal w/ the projections
        mergeConjunctions(ClientComputation(e, mkProjections(e), Seq.empty))
        cannotAnswerExpr
      }

      val newExpr = topDownTransformation(expr) {

        case Or(_, _, _) => 
          throw new Exception("TODO: don't assume clause is only conjuctive")
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
                  Seq(generatePlanFromOnionSet0(q, onionSet, SingleEncProj(Onions.DET)),
                      generatePlanFromOnionSet0(q, onionSet, SingleEncProj(Onions.OPE)))
                val sp0 = subPlans.map(x => if (x.isInstanceOf[RemoteSql]) Some(x) else None)
                CollectionUtils.optOr2(sp0(0), sp0(1)).map {
                  case RemoteSql(qp, _) =>
                    (Some(eq.copyWithChildren(Subselect(qp), NullLiteral())), false)
                }.getOrElse {
                  mergeConjunctions(
                    ClientComputation(
                      eq, 
                      Seq.empty, 
                      Seq((ss, generatePlanFromOnionSet0(q, onionSet, PreserveOriginal), PreserveOriginal))))
                  cannotAnswerExpr
                }
              case _ => 
                CollectionUtils.optOr2(
                  getSupportedExpr(lhs, Onions.DET),
                  getSupportedExpr(lhs, Onions.OPE))
                .map(fi => (Some(eq.copyWithChildren(fi, NullLiteral())), false))
                .getOrElse(cannotAnswer(eq))
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
                  generatePlanFromOnionSet0(ss.subquery, onionSet, SingleEncProj(Onions.DET)))

              val p1 = 
                e1.map(_ =>
                  generatePlanFromOnionSet0(ss.subquery, onionSet, SingleEncProj(Onions.OPE)))

              p0 match {
                case Some(RemoteSql(q0, _)) =>
                  (Some(eq.copyWithChildren(e0.get, Subselect(q0))), false)
                case _ =>
                  p1 match {
                    case Some(RemoteSql(q1, _)) =>
                      (Some(eq.copyWithChildren(e1.get, Subselect(q1))), false)
                    case _ =>
                      mergeConjunctions(
                        ClientComputation(
                          eq, 
                          mkProjections(expr),
                          Seq((ss, generatePlanFromOnionSet0(ss.subquery, onionSet, PreserveOriginal), PreserveOriginal))))
                      cannotAnswerExpr
                  }
              }
            }

            (lhs, rhs) match {
              case (ss0 @ Subselect(q0, _), ss1 @ Subselect(q1, _)) =>
                // try both DETs or both OPEs
                val spDETs = 
                  Seq(generatePlanFromOnionSet0(q0, onionSet, SingleEncProj(Onions.DET)),
                      generatePlanFromOnionSet0(q1, onionSet, SingleEncProj(Onions.DET)))
                val spOPEs = 
                  Seq(generatePlanFromOnionSet0(q0, onionSet, SingleEncProj(Onions.OPE)),
                      generatePlanFromOnionSet0(q1, onionSet, SingleEncProj(Onions.OPE)))

                // try spDETs first, then spOPEs
                (spDETs(0), spDETs(1)) match {
                  case (RemoteSql(q0p, _), RemoteSql(q1p, _)) =>
                    (Some(eq.copyWithChildren(Subselect(q0p), Subselect(q1p))), false)
                  case _ =>
                    (spOPEs(0), spOPEs(1)) match {
                      case (RemoteSql(q0p, _), RemoteSql(q1p, _)) =>
                        (Some(eq.copyWithChildren(Subselect(q0p), Subselect(q1p))), false)
                      case _ =>
                        mergeConjunctions(
                          ClientComputation(
                            eq, 
                            Seq.empty, 
                            Seq(
                              (ss0, generatePlanFromOnionSet0(q0, onionSet, PreserveOriginal), PreserveOriginal), 
                              (ss1, generatePlanFromOnionSet0(q1, onionSet, PreserveOriginal), PreserveOriginal))))
                        cannotAnswerExpr
                    }
                }
              case (ss @ Subselect(_, _), _) => handleOne(ss, rhs)
              case (_, ss @ Subselect(_, _)) => handleOne(ss, lhs)
              case _ =>
                CollectionUtils.optOr2(
                  CollectionUtils.opt2(
                    getSupportedExpr(lhs, Onions.DET),
                    getSupportedExpr(rhs, Onions.DET)),
                  CollectionUtils.opt2(
                    getSupportedExpr(lhs, Onions.OPE),
                    getSupportedExpr(rhs, Onions.OPE)))
                .map {
                  case (lfi, rfi) =>
                    (Some(eq.copyWithChildren(lfi, rfi)), false)
                }.getOrElse(cannotAnswer(eq))
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
              getSupportedExpr(lhs, Onions.OPE).map(fi => (Some(ieq.copyWithChildren(fi, NullLiteral())), false)).getOrElse(cannotAnswer(ieq))
            case (lhs, rhs) if lhs.isLiteral =>
              getSupportedExpr(rhs, Onions.OPE).map(fi => (Some(ieq.copyWithChildren(NullLiteral(), fi)), false)).getOrElse(cannotAnswer(ieq))
            case (lhs, rhs) =>
              CollectionUtils.opt2(
                getSupportedExpr(lhs, Onions.OPE),
                getSupportedExpr(rhs, Onions.OPE))
              .map {
                case (lfi, rfi) =>
                  (Some(ieq.copyWithChildren(lfi, rfi)), false)
              }.getOrElse(cannotAnswer(ieq))
          }

        case cs @ CountStar(_) if rewriteCtx.aggsValid => 
          onionRetVal.set(mkOnionRetVal(0))
          answerWithoutModification(cs)

        case m @ Min(f, _) if rewriteCtx.aggsValid =>
          onionRetVal.set(mkOnionRetVal(Onions.OPE))
          getSupportedExpr(f, Onions.OPE).map(fi => (Some(Min(fi)), false)).getOrElse(cannotAnswer(m))

        case m @ Max(f, _) if rewriteCtx.aggsValid =>
          onionRetVal.set(mkOnionRetVal(Onions.OPE))
          getSupportedExpr(f, Onions.OPE).map(fi => (Some(Max(fi)), false)).getOrElse(cannotAnswer(m))

        case s @ Sum(f, _, _) if rewriteCtx.aggsValid =>
          onionRetVal.set(mkOnionRetVal(Onions.HOM))
          getSupportedExpr(f, Onions.HOM).map(fi => (Some(AggCall("hom_add", Seq(fi))), false)).getOrElse(cannotAnswer(s))

        case avg @ Avg(f, _, _) if rewriteCtx.aggsValid =>
          onionRetVal.set(mkOnionRetVal(0))
          getSupportedExpr(f, Onions.HOM).map(fi => {
            val id0 = _generator.uniqueId()
            val id1 = _generator.uniqueId()

            val expr0 = FieldIdent(None, id0)
            val expr1 = FieldIdent(None, id1)

            val projs = Seq(
              (expr0, ExprProj(AggCall("hom_add", Seq(fi)), None), Onions.HOM), 
              (expr1, ExprProj(CountStar(), None), 0))

            mergeConjunctions(
              ClientComputation(
                Div(expr0, expr1),
                projs,
                Seq.empty))
            cannotAnswerExpr
          }).getOrElse(cannotAnswer(avg))

//        case gc @ GroupConcat(f, sep, _) =>
//          // always support this regardless of unagg or not
//          getSupportedExpr(f, Onions.DET).map(fi => (Some(GroupConcat(fi, sep)), false) ).getOrElse(cannotAnswer(gc))
//
        case e: SqlExpr =>

          def handleProj(onion: Int) = {
            val d = getSupportedExpr(e, Onions.DET)
            val o = getSupportedExpr(e, Onions.OPE)
            // try to honor the given onions first
            if ((onion & Onions.DET) != 0 && d.isDefined) {
              onionRetVal.set(mkOnionRetVal(Onions.DET))
              (Some(d.get), false)
            } else if ((onion & Onions.OPE) != 0 && o.isDefined) {
              onionRetVal.set(mkOnionRetVal(Onions.OPE))
              (Some(o.get), false)
            } else if (d.isDefined) {
              onionRetVal.set(mkOnionRetVal(Onions.DET))
              (Some(d.get), false)
            } else if (o.isDefined) {
              onionRetVal.set(mkOnionRetVal(Onions.OPE))
              (Some(o.get), false)
            } else {
              cannotAnswer(e)
            }
          }

          rewriteCtx match {
            case FilterCtx | GroupByHavingCtx =>
              throw new RuntimeException("TODO: need to evaluate in boolean context")
            case ProjCtx(o) => handleProj(o)
            case ProjUnaggCtx(o) => handleProj(o)
            case _ => throw new RuntimeException("TODO: impl")
          }
        
        case e => throw new Exception("should only have exprs under expr clause")
      }.asInstanceOf[SqlExpr]

      (newExpr, onionRetVal.get.getOrElse(0), conjunctions)
    }

    // filters
    cur = cur.filter.map(x => rewriteExprForServer(x, FilterCtx)).map {
      case (stmt, _, comps) =>
        val comps0 = 
          if (cur.groupBy.isDefined) {
            // need to group_concat the projections then, because we have a groupBy context
            comps.map { 
              case cc @ ClientComputation(_, p, _) =>
                cc.copy(projections = p.map {
                  case (expr, ExprProj(e, a, _), o) => 
                    // TODO: this isn't quite right
                    (expr, ExprProj(GroupConcat(e, ","), a), o)
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
          .getOrElse(throw new RuntimeException("TODO: currently cannot support non-field keys")))
      }))
      cur.copy(groupBy = newGroupBy0) 
    }

    // order by
    cur = {
      def handleUnsupported(o: SqlOrderBy) = {
        // TODO: general client side comp
        newLocalOrderBy ++= o.keys.map { case (f, _) => 
          CollectionUtils.optOrEither2(
            getSupportedExpr(f, Onions.OPE),
            getSupportedExpr(f, Onions.DET))
          .getOrElse(throw new RuntimeException("TODO: currently cannot support non-field keys")) match {
            case Left(fi) => 
              ClientComputation(f, Seq((f, ExprProj(fi, None), Onions.OPE)), Seq.empty)
            case Right(fi) =>
              ClientComputation(f, Seq((f, ExprProj(fi, None), Onions.DET)), Seq.empty)
          }
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
        comp.projections.map { case (_, p, o) =>
          val i = finalProjs.size
          finalProjs += ((p, o))
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

      cur.projections.foreach {
        case ExprProj(e, a, _) =>
          val onion = encContext match {
            case SingleEncProj(o) => o
            case _ => Onions.ALL
          }
          val ctx = 
            if (cur.groupBy.isDefined && !newLocalFilters.isEmpty) {
              ProjUnaggCtx(onion)
            } else {
              ProjCtx(onion) 
            }
          rewriteExprForServer(e, ctx) match {
            case (stmt, o, comps) =>
              // stmt first
              val stmtIdx = finalProjs.size
              finalProjs += ((ExprProj(stmt, a), o))

              // client comp
              val c0 = comps.map { c => 
                val m = processClientComputation(c)
                (c, m)
              }

              projPosMaps += ((stmtIdx, o, c0))
          }
        case StarProj(_) => throw new RuntimeException("TODO: implement me")
      }

      cur.copy(projections = finalProjs.map(_._1).toSeq)
    }

    def wrapDecryptionNodeSeq(p: PlanNode, m: Seq[Int]): PlanNode = {
      val td = p.tupleDesc
      val s = m.flatMap { pos =>
        if (td(pos).isDefined) Some(pos) else None
      }.toSeq
      LocalDecrypt(s, p) 
    }

    def wrapDecryptionNodeMap(p: PlanNode, m: Map[SqlProj, Int]): PlanNode = 
      wrapDecryptionNodeSeq(p, m.values.toSeq)

    val tdesc = 
      finalProjs.map(p => if (p._2 == 0) None else Some(p._2))
    val stage1 =  
      newLocalFilters.zip(localFilterPosMaps).foldLeft( RemoteSql(cur, tdesc) : PlanNode ) {
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
      // TODO: an optimization- if all columns are OPE, then don't need to decrypt

      // TODO: this is an oversimplification
      if (!newLocalOrderBy.isEmpty) {
        assert(newLocalOrderBy.size == stmt.orderBy.get.keys.size)
        val pos = newLocalOrderBy.zip(localOrderByPosMaps).map {
          case (comp, mapping) =>
            assert(mapping.size == 1) // TODO: assume for now
            assert(comp.projections.size == 1)
            mapping.values.head
        }
        LocalOrderBy(
          pos.zip(stmt.orderBy.get.keys).map { case (p, (_, t)) => (p, t) },
          LocalDecrypt(pos, stage2))
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
        case (_, _, Some((_, m))) => Some(m.values)
        case _ => None
      }.reduceLeft(_++_) ++ {
        projPosMaps.flatMap {
          case (p, o, None) if o != 0 =>
            Some(p)
          case _ => None
        }
      }
    ).toSeq

    val s0 = if (m.isEmpty) stage4 else wrapDecryptionNodeSeq(stage4, m)

    // 2) now do a final transformation
    val trfms = projPosMaps.map {
      case (_, _, Some((comp, mapping))) =>
        assert(comp.subqueries.isEmpty)
        Right(comp.mkSqlExpr(mapping))
      case (p, _, None) => Left(p)
    }

    val s1 = LocalTransform(trfms, s0)

    encContext match {
      case PreserveOriginal => s1
      case PreserveCardinality => stage4 // don't care about projections
      case SingleEncProj(o) =>
        assert(s1.tupleDesc.size == 1)
        // special case if stage4 is usuable
        val s4td = stage4.tupleDesc
        if (s4td.size == 1 && s4td.head.map(_ == o).getOrElse(false)) {
          stage4
        } else {
          s1.tupleDesc(0) match {
            case Some(o0) if o0 == o => s1

            case Some(o0) =>
              // decrypt -> encrypt
              LocalEncrypt(
                Seq((0, o)), 
                LocalDecrypt(
                  Seq(0),
                  s1))

            case None =>
              // encrypt
              LocalEncrypt(
                Seq((0, o)),
                s1)
          }
        }
    }
  }

  def generateCandidatePlans(stmt: SelectStmt): Seq[PlanNode] = {
    val o = generateOnionSets(stmt)
    val perms = CollectionUtils.powerSetMinusEmpty(o)
    // merge all perms, then unique
    val candidates = perms.map(p => OnionSet.merge(p)).toSet.toSeq
    println("size(candidates) = " + candidates.size)
    //println("candidates = " + candidates)
    def fillOnionSet(o: OnionSet): OnionSet = {
      o.complete(stmt.ctx.defns)
    }
    candidates.map(fillOnionSet).map(o => generatePlanFromOnionSet(stmt, o)).toSet.toSeq
  }

  def generateOnionSets(stmt: SelectStmt): Seq[OnionSet] = {

    def topDownTraverseContext(start: Node, ctx: Context)(f: Node => Boolean) = {
      topDownTraversal(start) {
        case e if e.ctx == ctx => f(e)
        case _ => false
      }
    }

    def traverseContext(
      start: Node, 
      ctx: Context, 
      bootstrap: Seq[OnionSet],
      selectFn: (SelectStmt, Seq[OnionSet]) => Seq[OnionSet]): Seq[OnionSet] = {

      var workingSet : Seq[OnionSet] = bootstrap

      def add1(sym: SqlExpr, o: Int): Boolean =
        findOnionableExpr(sym).map { 
          case (r, e) => workingSet.foreach(_.add(r, e, o)); true }.getOrElse(false)

      def add2(symL: SqlExpr, symR: SqlExpr, o: Int): Boolean = 
        CollectionUtils.opt2(
          findOnionableExpr(symL),
          findOnionableExpr(symR))
        .map {
          case ((l0, l1), (r0, r1)) =>
            workingSet.foreach(_.add(l0, l1, o))
            workingSet.foreach(_.add(r0, r1, o))
            true
        }.getOrElse(false)

      def negate(f: Node => Boolean): Node => Boolean = 
        (n: Node) => !f(n)

      topDownTraverseContext(start, ctx)(negate {
        case ieq: InequalityLike =>
          (ieq.lhs, ieq.rhs) match {
            case (lhs, rhs) if rhs.isLiteral =>
              add1(lhs, Onions.OPE)
            case (lhs, rhs) if lhs.isLiteral =>
              add1(rhs, Onions.OPE)
            case (lhs, rhs) =>
              add2(lhs, rhs, Onions.OPE)
          }

        case Like(lhs, rhs, _, _) if rhs.isLiteral =>
          add1(lhs, Onions.SWP)
        case Like(lhs, rhs, _, _) if lhs.isLiteral =>
          add1(rhs, Onions.SWP)
        case Like(lhs, rhs, _, _) =>
          add2(lhs, rhs, Onions.SWP)

        case Min(expr, _) =>
          add1(expr, Onions.OPE)
        case Max(expr, _) =>
          add1(expr, Onions.OPE)

        case Sum(expr, _, _) =>
          add1(expr, Onions.HOM)

        case Avg(expr, _, _) =>
          add1(expr, Onions.HOM)

        case SqlOrderBy(keys, _) =>
          CollectionUtils.optSeq(keys.map(k => findOnionableExpr(k._1))).map { _ => 
            keys.foreach(k => add1(k._1, Onions.OPE))
            true
          }.getOrElse(false)

        case ss : SelectStmt =>
          workingSet = selectFn(ss, workingSet)
          true

        //case e: SqlExpr =>
        //  add1(e, Onions.DET)

        case _ => false
      })

      workingSet
    }

    def buildForSelectStmt(stmt: SelectStmt, bootstrap: Seq[OnionSet]): Seq[OnionSet] = {
      val SelectStmt(p, _, f, g, o, _, ctx) = stmt
      val s0 = {
        var workingSet = bootstrap.map(_.copy)
        p.foreach { e =>
          workingSet = traverseContext(e, ctx, workingSet, buildForSelectStmt)
        }
        workingSet
      }
      
      val s1 = f.map(e => traverseContext(e, ctx, bootstrap.map(_.copy), buildForSelectStmt))

      val s2 = g.map(e => traverseContext(e, ctx, bootstrap.map(_.copy), buildForSelectStmt))

      val s3 = o.map(e => traverseContext(e, ctx, bootstrap.map(_.copy), buildForSelectStmt))

      s0 ++ s1.getOrElse(Seq.empty) ++ s2.getOrElse(Seq.empty) ++ s3.getOrElse(Seq.empty)
    }

    buildForSelectStmt(stmt, Seq(new OnionSet))
  }
}
