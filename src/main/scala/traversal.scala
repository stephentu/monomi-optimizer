trait Traversals {

  def topDownTraversal[A](n: Node)(f: Node => A): Unit =
    topDownTraversalWithParent(n)( (p: Option[Node], c: Node) => { f(c); true } )

  def topDownTraversalWithParent(n: Node)(f: (Option[Node], Node) => Boolean): Unit =
    topDownTraversal0(None, n)(f)

  def topDownTraversal0(p: Option[Node], n: Node)(f: (Option[Node], Node) => Boolean): Unit = {
    if (!f(p, n)) return
    def recur(n0: Node) = topDownTraversal0(Some(n), n0)(f)
    n match {
      case SelectStmt(p, r, f, g, o, _, _) =>
        p.map(recur); r.map(_.map(recur)); f.map(recur); g.map(recur); o.map(recur)
      case ExprProj(e, _, _) => recur(e)
      case Or(l, r, _) => recur(l); recur(r)
      case And(l, r, _) => recur(l); recur(r)
      case Eq(l, r, _) => recur(l); recur(r)
      case Neq(l, r, _) => recur(l); recur(r)
      case Ge(l, r, _) => recur(l); recur(r)
      case Gt(l, r, _) => recur(l); recur(r)
      case Le(l, r, _) => recur(l); recur(r)
      case Lt(l, r, _) => recur(l); recur(r)
      case In(e, Left(s), _, _) => recur(e); s.map(recur) 
      case In(e, Right(s), _, _) => recur(e); recur(s)
      case Like(l, r, _, _) => recur(l); recur(r)
      case Plus(l, r, _) => recur(l); recur(r)
      case Minus(l, r, _) => recur(l); recur(r)
      case Mult(l, r, _) => recur(l); recur(r)
      case Div(l, r, _) => recur(l); recur(r)
      case Not(e, _) => recur(e)
      case Exists(s, _) => recur(s)
      case Subselect(s, _) => recur(s)
      case CountExpr(e, _, _) => recur(e)
      case Sum(e, _, _) => recur(e)
      case Avg(e, _, _) => recur(e)
      case Min(e, _) => recur(e)
      case Max(e, _) => recur(e)
      case FunctionCall(_, a, _) => a.map(recur)
      case Extract(e, _, _) => recur(e)
      case Substring(e, _, _, _) => recur(e)
      case CaseExprCase(c, e, _) => recur(c); recur(e)
      case CaseExpr(e, c, d, _) => recur(e); c.map(recur); d.map(recur)
      case CaseWhenExpr(c, d, _) => c.map(recur); d.map(recur)
      case UnaryPlus(e, _) => recur(e)
      case UnaryMinus(e, _) => recur(e)
      case SubqueryRelationAST(s, _, _) => recur(s)
      case JoinRelation(l, r, _, c, _) => recur(l); recur(r); recur(c)
      case SqlGroupBy(_, h, _) => h.map(recur)
      case SqlOrderBy(k, _) => k.map(x => recur(x._1))
      case _ => 
    }
  }
}
