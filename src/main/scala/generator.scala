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
    
    val newProjections = new ArrayBuffer[Column]
    val newLocalFilters = new ArrayBuffer[SqlExpr]

    def keepGoing = (None, true)

    def enc(fi: FieldIdent, o: Int): FieldIdent = fi match {
      case FieldIdent(q, n, _, _) => FieldIdent(q.map(_ + "_enc"), n + "_" + Onions.str(o))
    }

    def cannotAnswer(expr: SqlExpr) = {
      newLocalFilters += expr      
      (Some(IntLiteral(1)), false)
    }

    val newFilter = stmt.filter.map(f => topDownTransformation(f) {
      case Or(_, _, _) => keepGoing
      case And(_, _, _) => keepGoing

      case eq: EqualityLike =>
        (eq.lhs, eq.rhs) match {
          case (l: FieldIdent, r: FieldIdent) =>
            // assume we can always handle this case
            (Some(eq.copyWithChildren(enc(l, Onions.DET), enc(l, Onions.DET))), false)
          case (l: FieldIdent, rhs) if rhs.isLiteral =>
            (Some(eq.copyWithChildren(enc(l, Onions.DET), NullLiteral() /* TODO: encrypt */)), false)

          case (lhs, r: FieldIdent) if lhs.isLiteral =>
            (Some(eq.copyWithChildren(NullLiteral() /* TODO: encrypt */, enc(r, Onions.DET))), false)

          case _ => cannotAnswer(eq)
        }

      case ieq: InequalityLike =>
        (ieq.lhs, ieq.rhs) match {
          case (l: FieldIdent, r: FieldIdent) =>
            val ok = 
              findOrigColumn(l.symbol).flatMap(l => findOrigColumn(r.symbol).map(r => (l, r))).map {
                case (l, r) =>
                  onionSet.opts.get(l).map(x => { 
                    (x & Onions.OPE) != 0 && 
                    onionSet.opts.get(r).map(y => (y & Onions.OPE) != 0).getOrElse(false) }).getOrElse(false)
              }.getOrElse(false)
            if (ok) (Some(ieq.copyWithChildren(enc(l, Onions.OPE), enc(r, Onions.OPE))), false)
            else cannotAnswer(ieq)

          case (l: FieldIdent, rhs) if rhs.isLiteral =>
            val ok = 
              findOrigColumn(l.symbol).flatMap(x => onionSet.opts.get(x)).map(y => (y & Onions.OPE) != 0).getOrElse(false)
            if (ok) (Some(ieq.copyWithChildren(enc(l, Onions.OPE), NullLiteral())), false)
            else cannotAnswer(ieq)

          case (lhs, r: FieldIdent) if lhs.isLiteral =>
            val ok = 
              findOrigColumn(r.symbol).flatMap(x => onionSet.opts.get(x)).map(y => (y & Onions.OPE) != 0).getOrElse(false)
            if (ok) (Some(ieq.copyWithChildren(NullLiteral(), enc(r, Onions.OPE))), false)
            else cannotAnswer(ieq)


          case _ => cannotAnswer(ieq)
        }
                 
      case e: SqlExpr =>
        // by default assume cannot answer
        cannotAnswer(e)
      
      case e => throw new Exception("should only have exprs under where clause")

    }.asInstanceOf[SqlExpr])

    newLocalFilters.foldLeft( RemoteSql(stmt.copy(filter = newFilter)) : PlanNode ) {
      case (acc, expr) => LocalFilter(expr, acc)
    }
  }

  def generateCandidatePlans(stmt: SelectStmt, schema: Map[String, Relation]): Seq[PlanNode] = {
    generateOnionSets(stmt, schema).map(o => generatePlanFromOnionSet(stmt, schema, o))
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
        s += new OnionSet(Onions.Projection)
        p.foreach(e => traverseContext(e, ctx, s.last))

        s += new OnionSet(Onions.Filter)
        f.foreach(e => traverseContext(e, ctx, s.last))

        s += new OnionSet(Onions.GroupBy)
        g.foreach(e => traverseContext(e, ctx, s.last))

        s += new OnionSet(Onions.OrderBy)
        o.foreach(e => traverseContext(e, ctx, s.last))
      case _ =>
    })

    s.toSeq
  }
}
