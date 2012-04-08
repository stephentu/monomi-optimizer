case class SelectStmt(val projections: Seq[SqlProj], 
                      val relations: Option[Seq[SqlRelation]],
                      val filter: Option[SqlExpr],
                      val groupBy: Option[SqlGroupBy],
                      val orderBy: Option[SqlOrderBy],
                      val limit: Option[Int]) 

abstract class SqlProj
case class ExprProj(expr: SqlExpr, alias: Option[String]) extends SqlProj
case object StarProj extends SqlProj

abstract class SqlExpr
case class Or(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class And(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr

case class Eq(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class Neq(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class Ge(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class Gt(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class Le(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class Lt(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr

case class In(elem: SqlExpr, set: Either[Seq[SqlExpr], SelectStmt]) extends SqlExpr
case class Like(lhs: SqlExpr, rhs: SqlExpr, negate: Boolean) extends SqlExpr

case class Plus(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class Minus(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr

case class Mult(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class Div(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr

case class Not(expr: SqlExpr) extends SqlExpr
case class Exists(select: SelectStmt) extends SqlExpr

case class FieldIdent(qualifier: Option[String], name: String) extends SqlExpr
case class Subselect(subquery: SelectStmt) extends SqlExpr

case object CountStar extends SqlExpr
case class CountExpr(expr: SqlExpr, distinct: Boolean) extends SqlExpr
case class Sum(expr: SqlExpr, distinct: Boolean) extends SqlExpr
case class Avg(expr: SqlExpr, distinct: Boolean) extends SqlExpr
case class Min(expr: SqlExpr) extends SqlExpr
case class Max(expr: SqlExpr) extends SqlExpr

case class FunctionCall(name: String, args: Seq[SqlExpr]) extends SqlExpr

sealed abstract trait ExtractType
case object YEAR extends ExtractType
case object MONTH extends ExtractType
case object DAY extends ExtractType

case class Extract(expr: SqlExpr, what: ExtractType) extends SqlExpr

case class Substring(expr: SqlExpr, from: Int, length: Option[Int]) extends SqlExpr

case class CaseExpr(expr: SqlExpr, cases: Seq[(SqlExpr, SqlExpr)], default: Option[SqlExpr]) extends SqlExpr
case class CaseWhenExpr(cases: Seq[(SqlExpr, SqlExpr)], default: Option[SqlExpr]) extends SqlExpr

case class UnaryPlus(expr: SqlExpr) extends SqlExpr
case class UnaryMinus(expr: SqlExpr) extends SqlExpr

case class IntLiteral(v: Long) extends SqlExpr
case class FloatLiteral(v: Double) extends SqlExpr
case class StringLiteral(v: String) extends SqlExpr
case object NullLiteral extends SqlExpr

case class DateLiteral(d: String) extends SqlExpr
case class IntervalLiteral(e: String, unit: ExtractType) extends SqlExpr

abstract class SqlRelation
case class TableRelation(name: String, alias: Option[String]) extends SqlRelation
case class SubqueryRelation(subquery: SelectStmt, alias: String) extends SqlRelation

sealed abstract trait JoinType
case class LeftJoin(outer: Boolean) extends JoinType
case class RightJoin(outer: Boolean) extends JoinType
case object InnerJoin extends JoinType

case class JoinRelation(left: SqlRelation, right: SqlRelation, tpe: JoinType, clause: SqlExpr) extends SqlRelation

sealed abstract trait OrderType
case object ASC extends OrderType
case object DESC extends OrderType

case class SqlGroupBy(keys: Seq[String], having: Option[SqlExpr]) 
case class SqlOrderBy(keys: Seq[(String, OrderType)]) 
