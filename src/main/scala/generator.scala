import scala.collection.mutable.ArrayBuffer

trait Generator extends Traversals with Transformers {

  private def findOrigColumn(c: Column): Option[(String, Column)] = c match {
    case t @ TableColumn(_, _, rlxn) => Some((rlxn, t))
    case AliasedColumn(_, orig) => findOrigColumn(orig)
    case v @ VirtualColumn(expr) => Some((v.relation, v))
    case _ => None
  }

  def generatePlanFromOnionSet(
    stmt: SelectStmt, schema: Map[String, Relation], onionSet: OnionSet): PlanNode = {

    // order is
    // 1) where clause (filter)
    // 2) group by 
    // 3) select clause (projections)
    // 4) order by

    // NOTE: assumes that all expressions are in disjunctive normal form (DNF)

    def columnSupports(c: Column, o: Int): Boolean = {
      findOrigColumn(c)
        .flatMap(x => onionSet.opts.get(x))
        .map(y => (y & o) != 0).getOrElse(false)
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

    def enc(fi: FieldIdent, o: Int): FieldIdent = fi match {
      case FieldIdent(q, n, _, _) => FieldIdent(q.map(_ + "_enc"), n + "_" + Onions.str(o))
    }

    // rewrite expr into an expr which can be evaluated on the server,
    // plus any additional projections,
    // plus an optional client operation which must be applied locally for reconstruction
    def rewriteExprForServer(expr: SqlExpr, unagg: Boolean): (SqlExpr, Option[(Seq[SqlProj], Option[SqlExpr])]) = {
      val projections = new ArrayBuffer[SqlProj]
      val conjunctions = new ArrayBuffer[SqlExpr]

      def cannotAnswer(e: SqlExpr) = {
        // filter out all fields
        assert(e.canGatherFields)

        val fields = e.gatherFields
        //println("cannot answer: " + e.sql)
        //println("  * fields: " + fields)

        // TODO: this seems too simplistic...
        val projs = fields.map(x => (x._1.name, x._1, x._2)).map { 
          case (name, col, false) =>
            ExprProj(FieldIdent(None, name, col), Some(_generator.uniqueId()))
          case (name, col, true) =>
            ExprProj(GroupConcat(FieldIdent(None, name, col), ","), Some(_generator.uniqueId()))
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
            case (l: FieldIdent, r: FieldIdent) =>
              // assume we can always handle this case
              // which might not be true (ie we might remove dets)
              (Some(eq.copyWithChildren(enc(l, Onions.DET), enc(l, Onions.DET))), false)
            case (l: FieldIdent, rhs) if rhs.isLiteral =>
              (Some(eq.copyWithChildren(enc(l, Onions.DET), NullLiteral() /* TODO: encrypt */)), false)
            case (lhs, r: FieldIdent) if lhs.isLiteral =>
              (Some(eq.copyWithChildren(NullLiteral() /* TODO: encrypt */, enc(r, Onions.DET))), false)
            case _ => cannotAnswer(eq)
          }

        case ieq: InequalityLike =>
          (ieq.lhs, ieq.rhs) match {
            case (l: FieldIdent, r: FieldIdent) 
              if columnSupports(l.symbol, Onions.OPE) && 
                 columnSupports(r.symbol, Onions.OPE) => 
              (Some(ieq.copyWithChildren(enc(l, Onions.OPE), enc(r, Onions.OPE))), false)
            case (l: FieldIdent, rhs) 
              if columnSupports(l.symbol, Onions.OPE) && rhs.isLiteral =>
              (Some(ieq.copyWithChildren(enc(l, Onions.OPE), NullLiteral())), false)
            case (lhs, r: FieldIdent) 
              if lhs.isLiteral && columnSupports(r.symbol, Onions.OPE) =>
              (Some(ieq.copyWithChildren(NullLiteral(), enc(r, Onions.OPE))), false)
            case _ => cannotAnswer(ieq)
          }

        case cs @ CountStar(_) if !unagg => 
          answerWithoutModification(cs)

        case Min(f @ FieldIdent(_, _, sym, _), _) if !unagg && columnSupports(sym, Onions.OPE) =>
          (Some(Min(enc(f, Onions.OPE))), false)

        case Max(f @ FieldIdent(_, _, sym, _), _) if !unagg && columnSupports(sym, Onions.OPE) =>
          (Some(Max(enc(f, Onions.OPE))), false)

        case Sum(f @ FieldIdent(_, _, sym, _), _, _) if !unagg && columnSupports(sym, Onions.HOM) =>
          (Some(AggCall("hom_add", Seq(enc(f, Onions.HOM)))), false)          

        case Sum(expr, _, _) if !unagg && columnSupports(VirtualColumn(expr), Onions.HOM) =>
          (Some(AggCall("hom_add", Seq(FieldIdent(None, "virtualColumn /*" + expr.sql + "*/")))), false)

        case avg @ Avg(f @ FieldIdent(_, _, sym, _), _, _) if !unagg && columnSupports(sym, Onions.HOM) =>
          val id = _generator.uniqueId()
          projections += ExprProj(CountStar(), Some(id))
          val aggCall = AggCall("hom_add", Seq(enc(f, Onions.HOM)))
          conjunctions += Div(aggCall, FieldIdent(None, id))
          (Some(aggCall), false)          

        case avg @ Avg(expr, _, _) if !unagg && columnSupports(VirtualColumn(expr), Onions.HOM) =>
          val id = _generator.uniqueId()
          projections += ExprProj(CountStar(), Some(id))
          val aggCall = AggCall("hom_add", Seq(FieldIdent(None, "virtualColumn /*" + expr.sql + "*/")))
          conjunctions += Div(aggCall, FieldIdent(None, id))
          (Some(aggCall), false)          

        case GroupConcat(f @ FieldIdent(_, _, sym, _), sep, _) =>
          // always support this regardless of unagg or not
          (Some(GroupConcat(enc(f, Onions.DET), sep)), false) 

        case f: FieldIdent =>
          (Some(enc(f, Onions.DET)), false)
                   
        case e: SqlExpr =>
          // by default assume cannot answer
          cannotAnswer(e)
        
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
            newLocalGroupBy.isEmpty &&
            o.keys.filter(k => !columnSupports(k._1.symbol, Onions.OPE)).isEmpty) {
          // can support server side order by
          Some(SqlOrderBy(o.keys.map(k => (enc(k._1, Onions.OPE), k._2))))
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
      def add1(sym: Column, o: Int) =
        findOrigColumn(sym).foreach(x => {
          onionSet.opts.put(x, onionSet.opts.getOrElse(x, 0) | o)})

      def add2(symL: Column, symR: Column, o: Int) = 
        findOrigColumn(symL).flatMap(l => findOrigColumn(symR).map(r => (l, r))).foreach {
          case (l, r) =>
            onionSet.opts.put(l, onionSet.opts.getOrElse(l, 0) | o)
            onionSet.opts.put(r, onionSet.opts.getOrElse(r, 0) | o)
        }

      topDownTraverseContext(start, ctx)(wrapReturnTrue {
        case ieq: InequalityLike =>
          (ieq.lhs, ieq.rhs) match {
            case (FieldIdent(_, _, symL, _), FieldIdent(_, _, symR, _)) =>
              add2(symL, symR, Onions.OPE)
            case (FieldIdent(_, _, symL, _), rhs) if rhs.isLiteral =>
              add1(symL, Onions.OPE)
            case (lhs, FieldIdent(_, _, symR, _)) if lhs.isLiteral =>
              add1(symR, Onions.OPE)
            case _ =>
          }

        case Like(FieldIdent(_, _, symL, _), FieldIdent(_, _, symR, _), _, _) =>
          add2(symL, symR, Onions.SWP)
        case Like(FieldIdent(_, _, symL, _), rhs, _, _) if rhs.isLiteral =>
          add1(symL, Onions.SWP)
        case Like(lhs, FieldIdent(_, _, symR, _), _, _) if lhs.isLiteral =>
          add1(symR, Onions.SWP)

        case Min(FieldIdent(_, _, sym, _), _) =>
          add1(sym, Onions.OPE)
        case Max(FieldIdent(_, _, sym, _), _) =>
          add1(sym, Onions.OPE)

        case Sum(FieldIdent(_, _, sym, _), _, _) =>
          add1(sym, Onions.HOM)
        case Sum(expr, _, _) if !expr.isLiteral =>
          expr.getPrecomputableRelation.foreach(x => add1(VirtualColumn(expr), Onions.HOM))

        case Avg(FieldIdent(_, _, sym, _), _, _) =>
          add1(sym, Onions.HOM)
        case Avg(expr, _, _) if !expr.isLiteral =>
          expr.getPrecomputableRelation.foreach(x => add1(VirtualColumn(expr), Onions.HOM))

        case SqlOrderBy(keys, _) =>
          keys.foreach(k => add1(k._1.symbol, Onions.OPE))

        // TODO: consider pre-computation for any general expression which
        // can be computed by only looking at a row within a single table 
        // (no cross table pre-computation)

        case _ =>
          
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
