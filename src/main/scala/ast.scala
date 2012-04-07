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
case class OrClause(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class AndClause(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr

case class EQClause(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class NEQClause(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr

case class GEClause(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class GTClause(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class LEClause(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class LTClause(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr

case class PlusClause(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class MinusClause(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr

case class MultClause(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr
case class DivClause(lhs: SqlExpr, rhs: SqlExpr) extends SqlExpr

case class NotClause(expr: SqlExpr) extends SqlExpr

case class FieldIdent(qualifier: Option[String], name: String) extends SqlExpr
case class FunctionCall(name: String, args: Seq[SqlExpr]) extends SqlExpr
case class Subselect(subquery: SelectStmt) extends SqlExpr

case class IntLiteral(v: Long) extends SqlExpr
case class StringLiteral(v: String) extends SqlExpr

abstract class SqlRelation
case class TableRelation(name: String, alias: Option[String]) extends SqlRelation
case class SubqueryRelation(subquery: SelectStmt, alias: String) extends SqlRelation
case class JoinRelation(left: SqlRelation, right: SqlRelation, clause: SqlExpr) extends SqlRelation

sealed abstract trait OrderType
case object ASC extends OrderType
case object DESC extends OrderType

case class SqlGroupBy(keys: Seq[String]) 
case class SqlOrderBy(keys: Seq[(String, OrderType)]) 
