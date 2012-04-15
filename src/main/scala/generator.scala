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

  def generatePlanFromOnionSet(
    stmt: SelectStmt, schema: Map[String, Relation], onionSet: OnionSet): PlanNode = {

    // order is
    // 1) where clause (filter)
    // 2) group by 
    // 3) select clause (projections)
    // 4) order by

    // NOTE: assumes that all expressions are in disjunctive normal form (DNF)

    def getSupportedExpr(e: SqlExpr, o: Int): Option[FieldIdent] = {
      val e0 = findOnionableExpr(e)
      e0.flatMap { x =>
        val key = (
          x._2.ctx.relations(x._1) match {
            case TableRelation(x) => x
            case _ => /* should not be here */
              throw new Exception("failure")
          },
          x._2)
        onionSet.opts.get(key).filter(y => (y._2 & o) != 0).map {
          case (basename, _) =>
            FieldIdent(Some(x._1), basename + "_" + Onions.str(o)) 
        }
      }
    }

    var cur = stmt // the current statement, as we update it

    val _generator = new NameGenerator("_projection")

    //val newProjections = new ArrayBuffer[SqlProj]

    val newLocalFilters = new ArrayBuffer[SqlExpr]

    // left is client group by, right is only client group filter
    var newLocalGroupBy: Option[Either[SqlGroupBy, SqlExpr]] = None

    val newLocalProjections = new ArrayBuffer[SqlExpr]

    var newLocalOrderBy: Option[Seq[(FieldIdent, OrderType)]] = None

    var newLocalLimit: Option[Int] = None

    def keepGoing = (None, true)
    def answerWithoutModification(n: Node) = (Some(n.copyWithContext(null)), false)

    // rewrite expr into an expr which can be evaluated on the server,
    // plus any additional projections,
    // plus an optional client operation which must be applied locally for reconstruction
    def rewriteExprForServer(expr: SqlExpr, unagg: Boolean): (SqlExpr, Option[(Seq[SqlProj], Option[SqlExpr])]) = {
      val projections = new ArrayBuffer[SqlProj]
      val conjunctions = new ArrayBuffer[SqlExpr]

      def cannotAnswer(e: SqlExpr): (Option[Node], Boolean) = {
        // filter out all fields
        assert(e.canGatherFields)

        val fields = e.gatherFields
        //println("cannot answer: " + e.sql)
        //println("  * fields: " + fields)

        val projs = fields.map { 
          case (f, false) =>
            ExprProj(f, Some(_generator.uniqueId()))
          case (f, true) =>
            ExprProj(GroupConcat(f, ","), Some(_generator.uniqueId()))
        }

        projections ++= projs

        // TODO: rewrite query deal w/ the projections
        conjunctions += e

        (Some(IntLiteral(1)), false)
      }

      val newExpr = topDownTransformation(expr) {

        case Or(_, _, _) => 
          throw new Exception("TODO: don't assume clause is only conjuctive")
        case And(_, _, _) => keepGoing

        case eq: EqualityLike =>
          (eq.lhs, eq.rhs) match {
            case (lhs, rhs) if rhs.isLiteral =>
              getSupportedExpr(lhs, Onions.DET).map(fi => (Some(eq.copyWithChildren(fi, NullLiteral())), false)).getOrElse(cannotAnswer(eq))
            case (lhs, rhs) if lhs.isLiteral =>
              getSupportedExpr(rhs, Onions.DET).map(fi => (Some(eq.copyWithChildren(NullLiteral(), fi)), false)).getOrElse(cannotAnswer(eq))
            case (lhs, rhs) =>
              CollectionUtils.opt2(
                getSupportedExpr(lhs, Onions.DET),
                getSupportedExpr(rhs, Onions.DET))
              .map {
                case (lfi, rfi) =>
                  (Some(eq.copyWithChildren(lfi, rfi)), false)
              }.getOrElse(cannotAnswer(eq))
          }

        case ieq: InequalityLike =>
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

        case cs @ CountStar(_) if !unagg => 
          answerWithoutModification(cs)

        case m @ Min(f, _) if !unagg =>
          getSupportedExpr(f, Onions.OPE).map(fi => (Some(Min(fi)), false)).getOrElse(cannotAnswer(m))

        case m @ Max(f, _) if !unagg =>
          getSupportedExpr(f, Onions.OPE).map(fi => (Some(Max(fi)), false)).getOrElse(cannotAnswer(m))

        case s @ Sum(f, _, _) if !unagg =>
          getSupportedExpr(f, Onions.HOM).map(fi => (Some(AggCall("hom_add", Seq(fi))), false)).getOrElse(cannotAnswer(s))

        case avg @ Avg(f, _, _) if !unagg =>
          getSupportedExpr(f, Onions.HOM).map(fi => {
            val id = _generator.uniqueId()
            projections += ExprProj(CountStar(), Some(id))
            val aggCall = AggCall("hom_add", Seq(fi))
            conjunctions += Div(aggCall, FieldIdent(None, id))
            (Some(aggCall), false)          
          }).getOrElse(cannotAnswer(avg))

        case gc @ GroupConcat(f, sep, _) =>
          // always support this regardless of unagg or not
          getSupportedExpr(f, Onions.DET).map(fi => (Some(GroupConcat(fi, sep)), false) ).getOrElse(cannotAnswer(gc))

        case e: SqlExpr =>
          getSupportedExpr(e, Onions.DET).map(fi => (Some(fi), false)).getOrElse(cannotAnswer(e))
        
        case e => throw new Exception("should only have exprs under where clause")
      }.asInstanceOf[SqlExpr]


      (newExpr, 
       if (projections.isEmpty && conjunctions.isEmpty) None
       else Some((
         projections.toSeq, 
         if (conjunctions.isEmpty) None else Some(conjunctions.reduceLeft((x: SqlExpr, y: SqlExpr) => {  
            And(x, y) 
         })))))
    }

    // filters
    cur = cur.filter.map(x => rewriteExprForServer(x, false)).map {
      case (stmt, Some((projs, expr))) =>
        val projsToAdd = 
          if (cur.groupBy.isDefined) {
            // need to group_concat the projections then
            projs.map { 
              case ExprProj(e, a, _) => ExprProj(GroupConcat(e, ","), a)
            }
          } else {
            projs
          }
        expr.foreach(e => newLocalFilters += e)
        cur.copy(projections = cur.projections ++ projsToAdd, filter = Some(stmt))
      case (stmt, None) => 
        cur.copy(filter = Some(stmt))
    }.getOrElse(cur)

    // group by
    cur = {
      // need to check if we can answer the having clause
      val (projsToAdd, newGroupBy) = 
        cur.groupBy.map(gb => gb.having.map(x => rewriteExprForServer(x, false)).map {
          case (stmt, Some((projs, expr))) =>
            newLocalGroupBy = expr.map(Right(_)) 
            (projs, Some(gb.copy(having = Some(stmt))))
          case (stmt, None) =>
            (Seq.empty, Some(gb.copy(having = Some(stmt))))
        }.getOrElse((Seq.empty, Some(gb)))).getOrElse((Seq.empty, None))

      val newGroupBy0 = newGroupBy.map(gb => gb.copy(keys = gb.keys.map(_ + "_DET")))
      cur.copy(projections = cur.projections ++ projsToAdd, groupBy = newGroupBy0) 
    }

    // projections
    cur = {
      var toProcess: Seq[SqlProj] = cur.projections
      var finalProjs: Seq[SqlProj] = Seq.empty
      while (!toProcess.isEmpty) {
        val thisIter = toProcess
        toProcess = Seq.empty
        finalProjs = finalProjs ++  
          thisIter
            .map {
              case ExprProj(e, a, _) =>
                rewriteExprForServer(e, cur.groupBy.isDefined && !newLocalFilters.isEmpty) match {
                  case (stmt, Some((projs, expr))) =>
                    toProcess ++= projs
                    expr.foreach(e => newLocalProjections += e)
                    ExprProj(stmt, a)
                  case (stmt, None) =>
                    ExprProj(stmt, a)
                }
              case StarProj(_) => 
                // TODO: the resolver phase should rewrite * to project all the fields,
                // so we only have to handle ExprProj
                throw new RuntimeException("unimpl")
            }
      }
      cur.copy(projections = finalProjs)
    }

    // order by
    cur = {
      val newOrderBy = cur.orderBy.flatMap(o => {
        if (newLocalFilters.isEmpty &&
            newLocalGroupBy.isEmpty) {

          val mapped = o.keys.map(f => (getSupportedExpr(f._1, Onions.OPE), f._2))
          if (mapped.map(_._1).flatten.size == mapped.size) {
            // can support server side order by
            Some(SqlOrderBy(mapped.map(f => (f._1.get, f._2))))
          } else {
            None
          }
        } else {
          newLocalOrderBy = Some(o.keys)
          None
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

    val stage1 =  
      newLocalFilters.foldLeft( RemoteSql(cur) : PlanNode ) {
        case (acc, expr) => LocalFilter(expr, acc)
      }

    val stage2 = 
      newLocalGroupBy.map(x => x match {
        case Left(_) => throw new RuntimeException("unimpl")
        case Right(expr) => LocalGroupFilter(expr, stage1)
      }).getOrElse(stage1)

    val stage3 = 
      newLocalProjections.foldLeft( stage2 ) {
        case (acc, expr) => LocalTransform(expr, acc)
      }

    val stage4 = 
      newLocalOrderBy.map(k => LocalOrderBy(k, stage3)).getOrElse(stage3)

    newLocalLimit.map(l => LocalLimit(l, stage4)).getOrElse(stage4)
  }

  def generateCandidatePlans(stmt: SelectStmt, schema: Map[String, Relation]): Seq[PlanNode] = {
    val o = generateOnionSets(stmt, schema)
    val perms = CollectionUtils.powerSetMinusEmpty(o)
    // merge all perms, then unique
    val candidates = perms.map(p => OnionSet.merge(p)).toSet.toSeq
    //println("size(candidates) = " + candidates.size)
    //println("candidates = " + candidates)
    candidates.map(o => generatePlanFromOnionSet(stmt, schema, o)).toSet.toSeq
  }

  def generateOnionSets(stmt: SelectStmt, schema: Map[String, Relation]): Seq[OnionSet] = {

    def topDownTraverseContext(start: Node, ctx: Context)(f: Node => Boolean) = {
      topDownTraversal(start) {
        case e if e.ctx == ctx => f(e)
        case _ => false
      }
    }

    def traverseContext(start: Node, ctx: Context, onionSet: OnionSet) = {
      def add1(sym: SqlExpr, o: Int) =
        findOnionableExpr(sym).foreach { case (r, e) => onionSet.add(r, e, o) }

      def add2(symL: SqlExpr, symR: SqlExpr, o: Int) = 
        CollectionUtils.opt2(
          findOnionableExpr(symL),
          findOnionableExpr(symR))
        .foreach {
          case ((l0, l1), (r0, r1)) =>
            onionSet.add(l0, l1, o)
            onionSet.add(r0, r1, o)
        }

      topDownTraverseContext(start, ctx)(wrapReturnTrue {
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
          CollectionUtils.optSeq(keys.map(k => findOnionableExpr(k._1))).foreach { _ => 
            keys.foreach(k => add1(k._1, Onions.OPE))
          }
        case e: SqlExpr =>
          add1(e, Onions.DET)
          
      })
    }

    val s = new ArrayBuffer[OnionSet]

    topDownTraversal(stmt)(wrapReturnTrue {
      case SelectStmt(p, _, f, g, o, _, ctx) =>
        s += new OnionSet
        p.foreach(e => traverseContext(e, ctx, s.last))

        s += new OnionSet
        f.foreach(e => traverseContext(e, ctx, s.last))

        s += new OnionSet
        g.foreach(e => traverseContext(e, ctx, s.last))

        s += new OnionSet
        o.foreach(e => traverseContext(e, ctx, s.last))
      case _ =>
    })

    s.toSeq
  }
}
