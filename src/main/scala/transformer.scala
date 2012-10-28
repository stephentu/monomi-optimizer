package edu.mit.cryptdb

trait PlanTransformers {

  def topDownTransformation(n: PlanNode)(f: PlanNode => (Option[PlanNode], Boolean)): PlanNode =
    topDownTransformationWithParent(n)( (p: Option[PlanNode], c: PlanNode) => f(c) )

  // order:
  // 1. visit node (call f() on node)
  // 2. recurse on children (if node changes, recurse on *new* children)
  // 3. if children change, replace node (but don't call f() again on the replaced node)
  //
  // assumptions: nodes don't change too much in structure ast-wise
  def topDownTransformationWithParent(n: PlanNode)(f: (Option[PlanNode], PlanNode) => (Option[PlanNode], Boolean)): PlanNode =
    topDownTransformation0(None, n)(f)

  def topDownTransformation0(p: Option[PlanNode], n: PlanNode)(f: (Option[PlanNode], PlanNode) => (Option[PlanNode], Boolean)): PlanNode = {
    val (nCopy, keepGoing) = f(p, n)
    val newNode = nCopy getOrElse n
    if (!keepGoing) return newNode
    def recur[N0 <: PlanNode](n0: N0) =
      topDownTransformation0(Some(newNode), n0)(f).asInstanceOf[N0]
    newNode match {
      case node @ RemoteSql(_, _, s, n) =>
        node.copy(subrelations = s.map(x => (recur(x._1), x._2)),
                  namedSubselects = n.map(x => (x._1, (recur(x._2._1), x._2._2))).toMap)
      case node @ RemoteMaterialize(_, c) => node.copy(child = recur(c))
      case node @ LocalOuterJoinFilter(_, _, _, c, s) => node.copy(child = recur(c), subqueries = s.map(recur))
      case node @ LocalFilter(_, _, c, s, _) => node.copy(child = recur(c), subqueries = s.map(recur))
      case node @ LocalTransform(_, _, c) => node.copy(child = recur(c))
      case node @ LocalGroupBy(_, _, _, _, c, s) => node.copy(child = recur(c), subqueries = s.map(recur))
      case node @ LocalGroupFilter(_, _, c, s) => node.copy(child = recur(c), subqueries = s.map(recur))
      case node @ LocalOrderBy(_, c) => node.copy(child = recur(c))
      case node @ LocalLimit(_, c) => node.copy(child = recur(c))
      case node @ LocalDecrypt(_, c) => node.copy(child = recur(c))
      case node @ LocalEncrypt(_, c) => node.copy(child = recur(c))
      case node @ LocalFlattener(c) => node.copy(child = recur(c))
      case e => e
    }
  }

}

trait Transformers {

  def topDownTransformation(n: Node)(f: Node => (Option[Node], Boolean)): Node =
    topDownTransformationWithParent(n)( (p: Option[Node], c: Node) => f(c) )

  // order:
  // 1. visit node (call f() on node)
  // 2. recurse on children (if node changes, recurse on *new* children)
  // 3. if children change, replace node (but don't call f() again on the replaced node)
  //
  // assumptions: nodes don't change too much in structure ast-wise
  def topDownTransformationWithParent(n: Node)(f: (Option[Node], Node) => (Option[Node], Boolean)): Node =
    topDownTransformation0(None, n)(f)

  def topDownTransformation0(p: Option[Node], n: Node)(f: (Option[Node], Node) => (Option[Node], Boolean)): Node = {
    val (nCopy, keepGoing) = f(p, n)
    val newNode = nCopy getOrElse n
    if (!keepGoing) return newNode
    def recur[N0 <: Node](n0: N0) =
      topDownTransformation0(Some(newNode), n0)(f).asInstanceOf[N0]
    newNode match {
      case node @ SelectStmt(p, r, f, g, o, _, _) =>
        node.copy(projections = p.map(recur),
                  relations = r.map(_.map(recur)),
                  filter = f.map(recur),
                  groupBy = g.map(recur),
                  orderBy = o.map(recur))
      case node @ ExprProj(e, _, _) => node.copy(expr = recur(e))
      case node @ Or(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ And(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Eq(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Neq(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Ge(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Gt(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Le(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Lt(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ In(e, s, _, _) => node.copy(elem = recur(e), set = s.map(recur))
      case node @ Like(l, r, _, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Plus(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Minus(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Mult(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Div(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
      case node @ Not(e, _) => node.copy(expr = recur(e))
      case node @ Exists(s, _) => node.copy(select = recur(s))
      case node @ Subselect(s, _) => node.copy(subquery = recur(s))
      case node @ CountExpr(e, _, _) => node.copy(expr = recur(e))
      case node @ Sum(e, _, _) => node.copy(expr = recur(e))
      case node @ Avg(e, _, _) => node.copy(expr = recur(e))
      case node @ Min(e, _) => node.copy(expr = recur(e))
      case node @ Max(e, _) => node.copy(expr = recur(e))
      case node @ GroupConcat(e, _, _, _) => node.copy(expr = recur(e))
      case node @ FunctionCall(_, a, _) => node.copy(args = a.map(recur))
      case node @ AggCall(_, a, _) => node.copy(args = a.map(recur))
      case node @ Extract(e, _, _) => node.copy(expr = recur(e))
      case node @ Substring(e, _, _, _) => node.copy(expr = recur(e))
      case node @ CaseExprCase(c, e, _) => node.copy(cond = recur(c), expr = recur(e))
      case node @ CaseExpr(e, c, d, _) =>
        node.copy(expr = recur(e), cases = c.map(recur), default = d.map(recur))
      case node @ CaseWhenExpr(c, d, _) =>
        node.copy(cases = c.map(recur), default = d.map(recur))
      case node @ UnaryPlus(e, _) => node.copy(expr = recur(e))
      case node @ UnaryMinus(e, _) => node.copy(expr = recur(e))
      case node @ SubqueryRelationAST(s, _, _) => node.copy(subquery = recur(s))
      case node @ JoinRelation(l, r, _, c, _) =>
        node.copy(left = recur(l), right = recur(r), clause = recur(c))
      case node @ SqlGroupBy(k, h, _) => node.copy(keys = k.map(recur), having = h.map(recur))
      case node @ SqlOrderBy(k, _) => node.copy(keys = k.map(x => (recur(x._1), x._2)))
      case e => e
    }
  }

  //def bottomUpTransformation(n: Node)(f: Node => Option[Node]): Node =
  //  bottomUpTransformationWithParent(n)( (p: Option[Node], c: Node) => f(c) )

  //def bottomUpTransformationWithParent(n: Node)(f: (Option[Node], Node) => Option[Node]): Node =
  //  bottomUpTransformation0(None, n)(f)

  //def bottomUpTransformation0(p: Option[Node], n: Node)(f: (Option[Node], Node) => Option[Node]): Node = {
  //  def recur[N0 <: Node](n0: N0) =
  //    bottomUpTransformation0(Some(n), n0)(f).asInstanceOf[N0]
  //  val n0 = n match {
  //    case node @ SelectStmt(p, r, f, g, o, _, _) =>
  //      node.copy(projections = p.map(recur),
  //                relations = r.map(_.map(recur)),
  //                filter = f.map(recur),
  //                groupBy = g.map(recur),
  //                orderBy = o.map(recur))
  //    case node @ ExprProj(e, _, _) => node.copy(expr = recur(e))
  //    case node @ Or(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ And(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ Eq(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ Neq(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ Ge(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ Gt(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ Le(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ Lt(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ In(e, Left(s), _, _) => node.copy(elem = recur(e), set = Left(s.map(recur)))
  //    case node @ In(e, Right(s), _, _) => node.copy(elem = recur(e), set = Right(recur(s)))
  //    case node @ Like(l, r, _, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ Plus(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ Minus(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ Mult(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ Div(l, r, _) => node.copy(lhs = recur(l), rhs = recur(r))
  //    case node @ Not(e, _) => node.copy(expr = recur(e))
  //    case node @ Exists(s, _) => node.copy(select = recur(s))
  //    case node @ Subselect(s, _) => node.copy(subquery = recur(s))
  //    case node @ CountExpr(e, _, _) => node.copy(expr = recur(e))
  //    case node @ Sum(e, _, _) => node.copy(expr = recur(e))
  //    case node @ Avg(e, _, _) => node.copy(expr = recur(e))
  //    case node @ Min(e, _) => node.copy(expr = recur(e))
  //    case node @ Max(e, _) => node.copy(expr = recur(e))
  //    case node @ FunctionCall(_, a, _) => node.copy(args = a.map(recur))
  //    case node @ Extract(e, _, _) => node.copy(expr = recur(e))
  //    case node @ Substring(e, _, _, _) => node.copy(expr = recur(e))
  //    case node @ CaseExprCase(c, e, _) => node.copy(cond = recur(c), expr = recur(e))
  //    case node @ CaseExpr(e, c, d, _) =>
  //      node.copy(expr = recur(e), cases = c.map(recur), default = d.map(recur))
  //    case node @ CaseWhenExpr(c, d, _) =>
  //      node.copy(cases = c.map(recur), default = d.map(recur))
  //    case node @ UnaryPlus(e, _) => node.copy(expr = recur(e))
  //    case node @ UnaryMinus(e, _) => node.copy(expr = recur(e))
  //    case node @ SubqueryRelationAST(s, _, _) => node.copy(subquery = recur(s))
  //    case node @ JoinRelation(l, r, _, c, _) =>
  //      node.copy(left = recur(l), right = recur(r), clause = recur(c))
  //    case node @ SqlGroupBy(_, h, _) => node.copy(having = h.map(recur))
  //    case node @ SqlOrderBy(k, _) => node.copy(keys = k.map(x => (recur(x._1), x._2)))
  //    case e => e
  //  }
  //  f(p, n0).getOrElse(n0)
  //}
}
