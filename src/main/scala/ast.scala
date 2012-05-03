package edu.mit.cryptdb

sealed abstract trait SqlDialect
case object MySQLDialect extends SqlDialect
case object PostgresDialect extends SqlDialect

trait Node extends PrettyPrinters {
  val ctx: Context

  def copyWithContext(ctx: Context): Node

  // emit sql repr of node
  def sql: String = sqlFromDialect(PostgresDialect)

  def sqlFromDialect(dialect: SqlDialect): String
}

case class SelectStmt(projections: Seq[SqlProj],
                      relations: Option[Seq[SqlRelation]],
                      filter: Option[SqlExpr],
                      groupBy: Option[SqlGroupBy],
                      orderBy: Option[SqlOrderBy],
                      limit: Option[Int], ctx: Context = null) extends Node {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) =
    Seq(Some("select"),
        Some(projections.map(_.sqlFromDialect(dialect)).mkString(", ")),
        relations.map(x => "from " + x.map(_.sqlFromDialect(dialect)).mkString(", ")),
        filter.map(x => "where " + x.sqlFromDialect(dialect)),
        groupBy.map(_.sqlFromDialect(dialect)),
        orderBy.map(_.sqlFromDialect(dialect)),
        limit.map(x => "limit " + x.toString)).flatten.mkString(" ")

  // is there some sort of grouping going on?
  def projectionsInAggContext: Boolean = {
    object traversal extends Traversals
    groupBy.isDefined || !projections.filter {
      case ExprProj(e, _, _) =>
        var foundAgg = false
        traversal.topDownTraversal(e) {
          case _: SqlAgg => foundAgg = true; false
          case _ => !foundAgg
        }
        foundAgg
      case _ => false
    }.isEmpty
  }
}

trait SqlProj extends Node
case class ExprProj(expr: SqlExpr, alias: Option[String], ctx: Context = null) extends SqlProj {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = Seq(Some(expr.sqlFromDialect(dialect)), alias).flatten.mkString(" as ")
}
case class StarProj(ctx: Context = null) extends SqlProj {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = "*"
}

trait SqlExpr extends Node {
  def getType: DataType = UnknownType
  def isLiteral: Boolean = false

  // is the r-value of this expression a literal?
  def isRValueLiteral: Boolean = isLiteral

  // return value is:
  // ( (table) relation name for this node's ctx, global table name )
  def getPrecomputableRelation: Option[(String, String)] = {
    val f = gatherFields
    if (f.isEmpty) return None

    // check no agg contexts
    if (!f.filter(_._2).isEmpty) return None

    // check all fields are not projection references
    // TODO: we *could* allow this later, as an optimization
    if (!f.filter(_._1.symbol.isInstanceOf[ProjectionSymbol]).isEmpty) return None

    // for precomputation, we require:
    // 1) all field ctxs are the same
    // 2) all field relations are the same

    // if fctx != ctx, then fctx *must* be a parent of ctx
    val fctx = {
      val s = f.map(_._1.symbol.ctx).toSet
      if (s.size > 1) None else Some(s.head)
    }

    val rlxns = {
      val s = f.map(_._1.symbol.asInstanceOf[ColumnSymbol].relation).toSet
      if (s.size > 1) None else Some(s.head)
    }

    // now check if the relation is not a subquery
    fctx.flatMap { f => rlxns.map(r => (f, r)) } flatMap {
      case (c, r) =>
        if (!c.relations.contains(r)) {
          println("bad expr = " + this)
        }
        c.relations(r) match {
          case TableRelation(n) => Some((r, n))
          case _ => None
        }
    }
  }

  // (col, true if aggregate context false otherwise)
  // only gathers fields within this context (
  // wont traverse into subselects )
  def gatherFields: Seq[(FieldIdent, Boolean)]
}

trait Binop extends SqlExpr {
  val lhs: SqlExpr
  val rhs: SqlExpr

  val opStr: String

  override def isLiteral = lhs.isLiteral && rhs.isLiteral
  def gatherFields = lhs.gatherFields ++ rhs.gatherFields

  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr): Binop

  def sqlFromDialect(dialect: SqlDialect) = Seq("(" + lhs.sqlFromDialect(dialect) + ")", opStr, "(" + rhs.sqlFromDialect(dialect) + ")") mkString " "
}

case class Or(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop {
  val opStr = "or"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class And(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop {
  val opStr = "and"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}

trait EqualityLike extends Binop
case class Eq(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends EqualityLike {
  val opStr = "="
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class Neq(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends EqualityLike {
  val opStr = "<>"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}

trait InequalityLike extends Binop
case class Ge(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends InequalityLike {
  val opStr = "<="
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class Gt(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends InequalityLike {
  val opStr = "<"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class Le(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends InequalityLike {
  val opStr = ">="
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class Lt(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends InequalityLike {
  val opStr = ">"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}

case class In(elem: SqlExpr, set: Seq[SqlExpr], negate: Boolean, ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  override def isLiteral =
    elem.isLiteral && set.filter(e => !e.isLiteral).isEmpty
  def gatherFields =
    elem.gatherFields ++ set.flatMap(_.gatherFields)
  def sqlFromDialect(dialect: SqlDialect) = Seq(elem.sqlFromDialect(dialect), "in", "(", set.map(_.sqlFromDialect(dialect)).mkString(", "), ")") mkString " "
}
case class Like(lhs: SqlExpr, rhs: SqlExpr, negate: Boolean, ctx: Context = null) extends Binop {
  val opStr = if (negate) "not like" else "like"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}

case class Plus(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop {
  val opStr = "+"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class Minus(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop {
  val opStr = "-"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}

case class Mult(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop {
  val opStr = "*"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class Div(lhs: SqlExpr, rhs: SqlExpr, ctx: Context = null) extends Binop {
  val opStr = "/"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: SqlExpr, rhs: SqlExpr) = copy(lhs = lhs, rhs = rhs, ctx = null)
}

trait Unop extends SqlExpr {
  val expr: SqlExpr
  val opStr: String
  override def isLiteral = expr.isLiteral
  def gatherFields = expr.gatherFields
  def sqlFromDialect(dialect: SqlDialect) = Seq(opStr, "(", expr.sqlFromDialect(dialect), ")") mkString " "
}

case class Not(expr: SqlExpr, ctx: Context = null) extends Unop {
  val opStr = "not"
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class Exists(select: Subselect, ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = Seq.empty
  def sqlFromDialect(dialect: SqlDialect) = Seq("exists", "(", select.sqlFromDialect(dialect), ")") mkString " "
}

case class FieldIdent(qualifier: Option[String], name: String, symbol: Symbol = null, ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(symbol = null, ctx = c)
  def gatherFields = Seq((this, false))
  def sqlFromDialect(dialect: SqlDialect) = Seq(qualifier, Some(name)).flatten.mkString(".")
}

case class Subselect(subquery: SelectStmt, ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = Seq.empty
  def sqlFromDialect(dialect: SqlDialect) = "(" + subquery.sqlFromDialect(dialect) + ")"
}

trait SqlAgg extends SqlExpr
case class CountStar(ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = Seq.empty
  def sqlFromDialect(dialect: SqlDialect) = "count(*)"
}
case class CountExpr(expr: SqlExpr, distinct: Boolean, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def sqlFromDialect(dialect: SqlDialect) = Seq(Some("count("), if (distinct) Some("distinct ") else None, Some(expr.sqlFromDialect(dialect)), Some(")")).flatten.mkString("")
}
case class Sum(expr: SqlExpr, distinct: Boolean, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def sqlFromDialect(dialect: SqlDialect) = Seq(Some("sum("), if (distinct) Some("distinct ") else None, Some(expr.sqlFromDialect(dialect)), Some(")")).flatten.mkString("")
}
case class Avg(expr: SqlExpr, distinct: Boolean, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def sqlFromDialect(dialect: SqlDialect) = Seq(Some("avg("), if (distinct) Some("distinct ") else None, Some(expr.sqlFromDialect(dialect)), Some(")")).flatten.mkString("")
}
case class Min(expr: SqlExpr, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def sqlFromDialect(dialect: SqlDialect) = "min(" + expr.sqlFromDialect(dialect) + ")"
}
case class Max(expr: SqlExpr, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def sqlFromDialect(dialect: SqlDialect) = "max(" + expr.sqlFromDialect(dialect) + ")"
}
case class GroupConcat(expr: SqlExpr, sep: String, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def sqlFromDialect(dialect: SqlDialect) = dialect match {
    case MySQLDialect =>
      Seq("group_concat(", Seq(expr.sqlFromDialect(dialect), quoteSingle(sep)).mkString(", "), ")").mkString("")
    case PostgresDialect =>
      Seq("array_to_string(array_agg(", expr.sqlFromDialect(dialect), "), ", quoteSingle(sep), ")").mkString("")
  }
}
case class AggCall(name: String, args: Seq[SqlExpr], ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = args.flatMap(_.gatherFields)
  def sqlFromDialect(dialect: SqlDialect) = Seq(name, "(", args.map(_.sqlFromDialect(dialect)).mkString(", "), ")").mkString("")
}

trait SqlFunction extends SqlExpr {
  val name: String
  val args: Seq[SqlExpr]
  override def isLiteral = args.foldLeft(true)(_ && _.isLiteral)
  def gatherFields = args.flatMap(_.gatherFields)
  def sqlFromDialect(dialect: SqlDialect) = Seq(name, "(", args.map(_.sqlFromDialect(dialect)) mkString ", ", ")") mkString ""
}

case class FunctionCall(name: String, args: Seq[SqlExpr], ctx: Context = null) extends SqlFunction {
  def copyWithContext(c: Context) = copy(ctx = c)
}

sealed abstract trait ExtractType
case object YEAR extends ExtractType
case object MONTH extends ExtractType
case object DAY extends ExtractType

case class Extract(expr: SqlExpr, what: ExtractType, ctx: Context = null) extends SqlFunction {
  val name = "extract"
  val args = Seq(expr)
  def copyWithContext(c: Context) = copy(ctx = c)
}

case class Substring(expr: SqlExpr, from: Int, length: Option[Int], ctx: Context = null) extends SqlFunction {
  val name = "substring"
  val args = Seq(expr)
  def copyWithContext(c: Context) = copy(ctx = c)
}

case class CaseExprCase(cond: SqlExpr, expr: SqlExpr, ctx: Context = null) extends Node {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = Seq("when", cond.sqlFromDialect(dialect), "then", expr.sqlFromDialect(dialect)) mkString " "
}
case class CaseExpr(expr: SqlExpr, cases: Seq[CaseExprCase], default: Option[SqlExpr], ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  override def isLiteral =
    expr.isLiteral &&
    cases.filter(x => !x.cond.isLiteral || !x.expr.isLiteral).isEmpty &&
    default.map(_.isLiteral).getOrElse(true)
  override def isRValueLiteral =
    cases.filterNot(_.expr.isRValueLiteral).isEmpty &&
    default.map(_.isRValueLiteral).getOrElse(true)
  def gatherFields =
    expr.gatherFields ++
    cases.flatMap(x => x.cond.gatherFields ++ x.expr.gatherFields) ++
    default.map(_.gatherFields).getOrElse(Seq.empty)
  def sqlFromDialect(dialect: SqlDialect) = Seq(Some("case"), Some(expr.sqlFromDialect(dialect)), Some(cases.map(_.sqlFromDialect(dialect)) mkString " "), default.map(d => "else " + d.sqlFromDialect(dialect)), Some("end")).flatten.mkString(" ")
}
case class CaseWhenExpr(cases: Seq[CaseExprCase], default: Option[SqlExpr], ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  override def isLiteral =
    cases.filter(x => !x.cond.isLiteral || !x.expr.isLiteral).isEmpty &&
    default.map(_.isLiteral).getOrElse(true)
  override def isRValueLiteral =
    cases.filterNot(_.expr.isRValueLiteral).isEmpty &&
    default.map(_.isRValueLiteral).getOrElse(true)
  def gatherFields =
    cases.flatMap(x => x.cond.gatherFields ++ x.expr.gatherFields) ++
    default.map(_.gatherFields).getOrElse(Seq.empty)
  def sqlFromDialect(dialect: SqlDialect) = Seq(Some("case"), Some(cases.map(_.sqlFromDialect(dialect)) mkString " "), default.map(d => "else " + d.sqlFromDialect(dialect)), Some("end")).flatten.mkString(" ")
}

case class UnaryPlus(expr: SqlExpr, ctx: Context = null) extends Unop {
  val opStr = "+"
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class UnaryMinus(expr: SqlExpr, ctx: Context = null) extends Unop {
  val opStr = "-"
  def copyWithContext(c: Context) = copy(ctx = c)
}

trait LiteralExpr extends SqlExpr {
  override def isLiteral = true
  def gatherFields = Seq.empty
}
case class IntLiteral(v: Long, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = v.toString
}
case class FloatLiteral(v: Double, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = v.toString
}
case class StringLiteral(v: String, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = quoteSingle(v)
}
case class NullLiteral(ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = "null"
}
case class DateLiteral(d: String, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = Seq("date", quoteSingle(d)) mkString " "
}
case class IntervalLiteral(e: String, unit: ExtractType, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = Seq("interval", quoteSingle(e), unit.toString) mkString " "
}

trait SqlRelation extends Node
case class TableRelationAST(name: String, alias: Option[String], ctx: Context = null) extends SqlRelation {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = Seq(Some(name), alias).flatten.mkString(" ")
}
case class SubqueryRelationAST(subquery: SelectStmt, alias: String, ctx: Context = null) extends SqlRelation {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = Seq("(", subquery.sqlFromDialect(dialect), ")", "as", alias) mkString " "
}

sealed abstract trait JoinType {
  def sql: String = sqlFromDialect(PostgresDialect)
  def sqlFromDialect(dialect: SqlDialect): String
}
case object LeftJoin extends JoinType {
  def sqlFromDialect(dialect: SqlDialect) = "left join"
}
case object RightJoin extends JoinType {
  def sqlFromDialect(dialect: SqlDialect) = "right join"
}
case object InnerJoin extends JoinType {
  def sqlFromDialect(dialect: SqlDialect) = "join"
}

case class JoinRelation(left: SqlRelation, right: SqlRelation, tpe: JoinType, clause: SqlExpr, ctx: Context = null) extends SqlRelation {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = Seq(left.sqlFromDialect(dialect), tpe.sqlFromDialect(dialect), right.sqlFromDialect(dialect), "on", "(", clause.sqlFromDialect(dialect), ")") mkString " "
}

sealed abstract trait OrderType
case object ASC extends OrderType
case object DESC extends OrderType

case class SqlGroupBy(keys: Seq[SqlExpr], having: Option[SqlExpr], ctx: Context = null) extends Node {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = Seq(Some("group by"), Some(keys.map(_.sqlFromDialect(dialect)).mkString(", ")), having.map(e => "having " + e.sqlFromDialect(dialect))).flatten.mkString(" ")
}
case class SqlOrderBy(keys: Seq[(SqlExpr, OrderType)], ctx: Context = null) extends Node {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sqlFromDialect(dialect: SqlDialect) = Seq("order by", keys map (x => x._1.sqlFromDialect(dialect) + " " + x._2.toString) mkString ", ") mkString " "
}

// synthetic nodes

case class TuplePosition(pos: Int, ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = throw new RuntimeException("error")
  def sqlFromDialect(dialect: SqlDialect) = "pos$" + pos
}

// this node shouldn't appear in a re-written subquery
// it is merely a placeholder
case class DependentFieldPlaceholder(pos: Int, ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = Seq.empty
  def sqlFromDialect(dialect: SqlDialect) = "<should_not_appear>$" + pos
  def bind(onion: Int): BoundDependentFieldPlaceholder =
    BoundDependentFieldPlaceholder(pos, onion, ctx)
}

case class BoundDependentFieldPlaceholder(pos: Int, onion: Int, ctx: Context = null)
  extends SqlExpr {
  assert(BitUtils.onlyOne(onion))
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = throw new RuntimeException("error")
  def sqlFromDialect(dialect: SqlDialect) = "param$" + pos + "$" + Onions.str(onion)
}

case class SubqueryPosition(pos: Int, args: Seq[SqlExpr], ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = throw new RuntimeException("error")
  def sqlFromDialect(dialect: SqlDialect) = (Seq("subquery$" + pos, "(", args.map(_.sqlFromDialect(dialect)).mkString(", "), ")")).mkString("")
}

case class ExistsSubqueryPosition(pos: Int, args: Seq[SqlExpr], ctx: Context = null) extends SqlExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = throw new RuntimeException("error")
  def sqlFromDialect(dialect: SqlDialect) = (Seq("exists(subquery$" + pos, "(", args.map(_.sqlFromDialect(dialect)).mkString(", "), "))")).mkString("")
}
