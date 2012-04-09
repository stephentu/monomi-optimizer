trait Traversals {
  def wrap[A](f: Node => A): (Node => Node) = (n: Node) => {
    f(n)
    n
  }

  def traverse(n: Node)(trfm: Node => Node): Node = {
    def _gen(x: Node, a: AnyRef): AnyRef = null
    def _trfm(x: Node, a: AnyRef): Node = trfm(x)
    traverseWithContext(n, null.asInstanceOf[AnyRef])(_gen)(_trfm)
  }
  
  def traverseWithContext[A](n: Node, _a: A)(gen: (Node, A) => A)(trfm: (Node, A) => Node): Node = {
    val a_ctx = gen(n, _a)
    def recur[B <: Node](n: B): B = traverseWithContext(n, a_ctx)(gen)(trfm).asInstanceOf[B]
    n match {
      case SelectStmt(p, r, f, g, o, l, ctx) =>
        val p0 = p.map(recur)
        val r0 = r.map(_.map(recur))
        val f0 = f.map(recur)
        val g0 = g.map(recur)
        val o0 = o.map(recur)
        trfm(SelectStmt(p0, r0, f0, g0, o0, l, ctx), a_ctx)
      case ExprProj(e, a, ctx) => trfm(ExprProj(recur(e), a, ctx), a_ctx)
      case Or(l, r, ctx) => trfm(Or(recur(l), recur(r), ctx), a_ctx)
      case And(l, r, ctx) => trfm(And(recur(l), recur(r), ctx), a_ctx)
      case Eq(l, r, ctx) => trfm(Eq(recur(l), recur(r), ctx), a_ctx)
      case Neq(l, r, ctx) => trfm(Neq(recur(l), recur(r), ctx), a_ctx)
      case Ge(l, r, ctx) => trfm(Ge(recur(l), recur(r), ctx), a_ctx)
      case Gt(l, r, ctx) => trfm(Gt(recur(l), recur(r), ctx), a_ctx)
      case Le(l, r, ctx) => trfm(Le(recur(l), recur(r), ctx), a_ctx)
      case Lt(l, r, ctx) => trfm(Lt(recur(l), recur(r), ctx), a_ctx)
      case In(e, Left(s), n, ctx) => trfm(In(recur(e), Left(s.map(recur)), n, ctx), a_ctx)
      case In(e, Right(s), n, ctx) => trfm(In(recur(e), Right(recur(s)), n, ctx), a_ctx)
      case Like(l, r, n, ctx) => trfm(Like(recur(l), recur(r), n, ctx), a_ctx)
      case Plus(l, r, ctx) => trfm(Plus(recur(l), recur(r), ctx), a_ctx)
      case Minus(l, r, ctx) => trfm(Minus(recur(l), recur(r), ctx), a_ctx)
      case Mult(l, r, ctx) => trfm(Mult(recur(l), recur(r), ctx), a_ctx)
      case Div(l, r, ctx) => trfm(Div(recur(l), recur(r), ctx), a_ctx)
      case Not(e, ctx) => trfm(Not(recur(e), ctx), a_ctx)
      case Exists(s, ctx) => trfm(Exists(recur(s), ctx), a_ctx)
      case Subselect(s, ctx) => trfm(Subselect(recur(s), ctx), a_ctx)
      case CountExpr(e, d, ctx) => trfm(CountExpr(recur(e), d, ctx), a_ctx)
      case Sum(e, d, ctx) => trfm(Sum(recur(e), d, ctx), a_ctx)
      case Avg(e, d, ctx) => trfm(Avg(recur(e), d, ctx), a_ctx)
      case Min(e, ctx) => trfm(Min(recur(e), ctx), a_ctx)
      case Max(e, ctx) => trfm(Max(recur(e), ctx), a_ctx)
      case FunctionCall(n, a, ctx) => trfm(FunctionCall(n, a.map(recur), ctx), a_ctx)
      case Extract(e, w, ctx) => trfm(Extract(recur(e), w, ctx), a_ctx)
      case Substring(e, frm, l, ctx) => trfm(Substring(recur(e), frm, l, ctx), a_ctx)
      case CaseExprCase(c, e, ctx) => trfm(CaseExprCase(recur(c), recur(e), ctx), a_ctx)
      case CaseExpr(e, c, d, ctx) => trfm(CaseExpr(recur(e), c.map(recur), d.map(recur), ctx), a_ctx)
      case CaseWhenExpr(c, d, ctx) => trfm(CaseWhenExpr(c.map(recur), d.map(recur), ctx), a_ctx)
      case UnaryPlus(e, ctx) => trfm(UnaryPlus(recur(e), ctx), a_ctx)
      case UnaryMinus(e, ctx) => trfm(UnaryMinus(recur(e), ctx), a_ctx)
      case SubqueryRelationAST(s, a, ctx) => trfm(SubqueryRelationAST(recur(s), a, ctx), a_ctx)
      case JoinRelation(l, r, t, c, ctx) => trfm(JoinRelation(recur(l), recur(r), t, recur(c), ctx), a_ctx)
      case SqlGroupBy(k, h, ctx) => trfm(SqlGroupBy(k, h.map(recur), ctx), a_ctx)
      case SqlOrderBy(k, ctx) => trfm(SqlOrderBy(k.map(x => (recur(x._1), x._2)), ctx), a_ctx)
      case e => trfm(e, a_ctx)
    }
  }
}
