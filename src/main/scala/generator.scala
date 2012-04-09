import scala.collection.mutable.ArrayBuffer

trait Generator extends Traversals {
  def generatePlanFromOnionSet(
    stmt: SelectStmt, schema: Map[String, Relation], onionSet: OnionSet): PlanNode = {

    // order is
    // 1) where clause (filter)
    // 2) group by 
    // 3) select clause (projections)
    // 4) order by

    // 1) collect clauses which cannot be answered, and replace nodes w/ 
    
    throw new Exception("unimpl")
  }

  def generateCandidatePlans(stmt: SelectStmt, schema: Map[String, Relation]): Seq[PlanNode] = {
    generateOnionSets(stmt, schema).map(o => generatePlanFromOnionSet(stmt, schema, o))
  }

  def generateOnionSets(stmt: SelectStmt, schema: Map[String, Relation]): Seq[OnionSet] = {

    def findOrigColumn(c: Column): Option[(String, Column)] = c match {
      case t @ TableColumn(_, _, rlxn) => Some((rlxn, t))
      case AliasedColumn(_, orig) => findOrigColumn(orig)
      case v @ VirtualColumn(expr) => Some((v.relation, v))
      case _ => None
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

      topDownTraversal(start) {
        case Ge(FieldIdent(_, _, symL, _), FieldIdent(_, _, symR, _), ctx0) if ctx == ctx0 =>
          add2(symL, symR, Onions.OPE)
        case Ge(FieldIdent(_, _, symL, _), rhs, ctx0) if ctx == ctx0 && rhs.isLiteral =>
          add1(symL, Onions.OPE)
        case Ge(lhs, FieldIdent(_, _, symR, _), ctx0) if ctx == ctx0 && lhs.isLiteral =>
          add1(symR, Onions.OPE)
        case Gt(FieldIdent(_, _, symL, _), FieldIdent(_, _, symR, _), ctx0) if ctx == ctx0 =>
          add2(symL, symR, Onions.OPE)
        case Gt(FieldIdent(_, _, symL, _), rhs, ctx0) if ctx == ctx0 && rhs.isLiteral =>
          add1(symL, Onions.OPE)
        case Gt(lhs, FieldIdent(_, _, symR, _), ctx0) if ctx == ctx0 && lhs.isLiteral =>
          add1(symR, Onions.OPE)
        case Le(FieldIdent(_, _, symL, _), FieldIdent(_, _, symR, _), ctx0) if ctx == ctx0 =>
          add2(symL, symR, Onions.OPE)
        case Le(FieldIdent(_, _, symL, _), rhs, ctx0) if ctx == ctx0 && rhs.isLiteral =>
          add1(symL, Onions.OPE)
        case Le(lhs, FieldIdent(_, _, symR, _), ctx0) if ctx == ctx0 && lhs.isLiteral =>
          add1(symR, Onions.OPE)
        case Lt(FieldIdent(_, _, symL, _), FieldIdent(_, _, symR, _), ctx0) if ctx == ctx0 =>
          add2(symL, symR, Onions.OPE)
        case Lt(FieldIdent(_, _, symL, _), rhs, ctx0) if ctx == ctx0 && rhs.isLiteral =>
          add1(symL, Onions.OPE)
        case Lt(lhs, FieldIdent(_, _, symR, _), ctx0) if ctx == ctx0 && lhs.isLiteral =>
          add1(symR, Onions.OPE)

        case Like(FieldIdent(_, _, symL, _), FieldIdent(_, _, symR, _), _, ctx0) if ctx == ctx0 =>
          add2(symL, symR, Onions.OPE)
        case Like(FieldIdent(_, _, symL, _), rhs, _, ctx0) if ctx == ctx0 && rhs.isLiteral =>
          add1(symL, Onions.OPE)
        case Like(lhs, FieldIdent(_, _, symR, _), _, ctx0) if ctx == ctx0 && lhs.isLiteral =>
          add1(symR, Onions.OPE)

        case Min(FieldIdent(_, _, sym, _), ctx0) if ctx == ctx0 =>
          add1(sym, Onions.OPE)
        case Max(FieldIdent(_, _, sym, _), ctx0) if ctx == ctx0 =>
          add1(sym, Onions.OPE)

        case Sum(FieldIdent(_, _, sym, _), _, ctx0) if ctx == ctx0 =>
          add1(sym, Onions.HOM)
        case Sum(expr, _, ctx0) if ctx == ctx0 && !expr.isLiteral =>
          expr.getPrecomputableRelation.foreach(x => add1(VirtualColumn(expr), Onions.HOM))

        case Avg(FieldIdent(_, _, sym, _), _, ctx0) if ctx == ctx0 =>
          add1(sym, Onions.HOM)
        case Avg(expr, _, ctx0) if ctx == ctx0 && !expr.isLiteral =>
          expr.getPrecomputableRelation.foreach(x => add1(VirtualColumn(expr), Onions.HOM))

        case SqlOrderBy(keys, ctx0) if ctx == ctx0 =>
          keys.foreach(k => add1(k._1.symbol, Onions.OPE))

        case _ =>
          
      }
    }

    val s = new ArrayBuffer[OnionSet]

    topDownTraversal(stmt) {
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
    }

    s.toSeq
  }
}
