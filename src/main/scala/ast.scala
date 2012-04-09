trait Node {
  def copyWithContext(ctx: Context): Node
}

case class SelectStmt(projections: Seq[SqlProj], 
                      relations: Option[Seq[SqlRelation]],
                      filter: Option[SqlExpr],
                      groupBy: Option[SqlGroupBy],
                      orderBy: Option[SqlOrderBy],
                      limit: Option[Int], ctx: Context = null) extends Node {
  def copyWithContext(c: Context) = copy(ctx = c)
}

abstract class SqlProj extends Node
case class ExprProj(expr: SqlExpr, alias: Option[String], ctx: Context = null) extends SqlProj {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class StarProj(ctx: Context = null) extends SqlProj {
  def copyWithContext(c: Context) = copy(ctx = c)
}

abstract class SqlExpr extends Node {
  def isLiteral: Boolean = false
  def getPrecomputableRelation: Option[String] = {
    if (!canGatherFields) None
    val f = gatherFields
    if (!f.filter(_.isInstanceOf[ExprColumn]).isEmpty) None

    def extractRelation(c: Column): String = c match {
      case TableColumn(_, _, relation) => relation
      case AliasedColumn(_, orig) => extractRelation(orig)
    }

    val rxs = f.map(extractRelation).toSet
    if (rxs.size == 1) Some(rxs.head) else None
  }
  def canGatherFields: Boolean
  def gatherFields: Seq[Column]
}

abstract class Binop(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr {
  override def isLiteral = lhs.isLiteral && rhs.isLiteral
  def canGatherFields = lhs.canGatherFields && rhs.canGatherFields
  def gatherFields = lhs.gatherFields ++ rhs.gatherFields
}

case class Or(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class And(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}

case class Eq(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Neq(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Ge(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Gt(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Le(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Lt(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}

case class In(elem: SqlExpr, set: Either[Seq[SqlExpr], SelectStmt], negate: Boolean, ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  override def isLiteral = 
    elem.isLiteral && 
    set.left.toOption.map(_.filter(e => !e.isLiteral).isEmpty).getOrElse(false)
  def canGatherFields = 
    elem.canGatherFields && 
    set.left.toOption.map(_.foldLeft(true)(_&&_.canGatherFields)).getOrElse(false)
  def gatherFields =
    elem.gatherFields ++ set.left.get.flatMap(_.gatherFields)
}
case class Like(lhs: SqlExpr, rhs: SqlExpr, negate: Boolean, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}

case class Plus(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Minus(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}

case class Mult(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Div(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop(lhs, rhs) {
  def copyWithContext(c: Context) = copy(ctx = c)
}

abstract class Unop(expr: SqlExpr) extends SqlExpr {
  override def isLiteral = expr.isLiteral
  def canGatherFields = expr.canGatherFields 
  def gatherFields = expr.gatherFields
}

case class Not(expr: SqlExpr, ctx: Context = null) extends Unop(expr) {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Exists(select: SelectStmt, ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def canGatherFields = false
  def gatherFields = throw new RuntimeException("not possible")
}

case class FieldIdent(qualifier: Option[String], name: String, symbol: Column = null, ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def canGatherFields = true
  def gatherFields = Seq(symbol)
}
case class Subselect(subquery: SelectStmt, ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def canGatherFields = false
  def gatherFields = throw new RuntimeException("not possible")
}

abstract class SqlAgg extends SqlExpr {
  def canGatherFields = false
  def gatherFields = throw new RuntimeException("not possible")
}
case class CountStar(ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class CountExpr(expr: SqlExpr, distinct: Boolean, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Sum(expr: SqlExpr, distinct: Boolean, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Avg(expr: SqlExpr, distinct: Boolean, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Min(expr: SqlExpr, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Max(expr: SqlExpr, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
}

abstract class SqlFunction(name: String, args: Seq[SqlExpr]) extends SqlExpr {
  override def isLiteral = args.foldLeft(true)(_ && _.isLiteral) 
  def canGatherFields = args.foldLeft(true)(_ && _.canGatherFields) 
  def gatherFields = args.flatMap(_.gatherFields) 
}
case class FunctionCall(name: String, args: Seq[SqlExpr], ctx: Context = null) extends SqlFunction(name, args) {
  def copyWithContext(c: Context) = copy(ctx = c)
}

sealed abstract trait ExtractType
case object YEAR extends ExtractType
case object MONTH extends ExtractType
case object DAY extends ExtractType

case class Extract(expr: SqlExpr, what: ExtractType, ctx: Context = null) extends SqlFunction("extract", Seq(expr)) {
  def copyWithContext(c: Context) = copy(ctx = c)
}

case class Substring(expr: SqlExpr, from: Int, length: Option[Int], ctx: Context = null) extends SqlFunction("substring", Seq(expr)) {
  def copyWithContext(c: Context) = copy(ctx = c)
}

case class CaseExprCase(cond: SqlExpr, expr: SqlExpr, ctx: Context = null) extends Node {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class CaseExpr(expr: SqlExpr, cases: Seq[CaseExprCase], default: Option[SqlExpr], ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  override def isLiteral = 
    expr.isLiteral && 
    cases.filter(x => !x.cond.isLiteral || !x.expr.isLiteral).isEmpty
  def canGatherFields =
    expr.canGatherFields &&
    cases.filter(x => !x.cond.canGatherFields || !x.expr.canGatherFields).isEmpty
  def gatherFields =
    expr.gatherFields ++
    cases.flatMap(x => x.cond.gatherFields ++ x.expr.gatherFields)
}
case class CaseWhenExpr(cases: Seq[CaseExprCase], default: Option[SqlExpr], ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  override def isLiteral = 
    cases.filter(x => !x.cond.isLiteral || !x.expr.isLiteral).isEmpty
  def canGatherFields =
    cases.filter(x => !x.cond.canGatherFields || !x.expr.canGatherFields).isEmpty
  def gatherFields =
    cases.flatMap(x => x.cond.gatherFields ++ x.expr.gatherFields)
}

case class UnaryPlus(expr: SqlExpr, ctx: Context = null) extends Unop(expr) {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class UnaryMinus(expr: SqlExpr, ctx: Context = null) extends Unop(expr) {
  def copyWithContext(c: Context) = copy(ctx = c)
}

abstract class LiteralExpr extends SqlExpr {
  override def isLiteral = true
  def canGatherFields = true 
  def gatherFields = Seq.empty
}
case class IntLiteral(v: Long, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class FloatLiteral(v: Double, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class StringLiteral(v: String, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class NullLiteral(ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class DateLiteral(d: String, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class IntervalLiteral(e: String, unit: ExtractType, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
}

abstract class SqlRelation extends Node
case class TableRelationAST(name: String, alias: Option[String], ctx: Context = null) extends SqlRelation {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class SubqueryRelationAST(subquery: SelectStmt, alias: String, ctx: Context = null) extends SqlRelation {
  def copyWithContext(c: Context) = copy(ctx = c)
}

sealed abstract trait JoinType
case class LeftJoin(outer: Boolean) extends JoinType
case class RightJoin(outer: Boolean) extends JoinType
case object InnerJoin extends JoinType

case class JoinRelation(left: SqlRelation, right: SqlRelation, tpe: JoinType, clause: SqlExpr, ctx: Context = null) extends SqlRelation {
  def copyWithContext(c: Context) = copy(ctx = c)
}

sealed abstract trait OrderType
case object ASC extends OrderType
case object DESC extends OrderType

case class SqlGroupBy(keys: Seq[String], having: Option[SqlExpr], ctx: Context = null) extends Node {
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class SqlOrderBy(keys: Seq[(FieldIdent, OrderType)], ctx: Context = null) extends Node {
  def copyWithContext(c: Context) = copy(ctx = c)
}
