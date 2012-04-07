import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._

class SQLParser extends StandardTokenParsers {

  lexical.reserved += (
    "select", "as", "or", "and", "group", "order", "by", "where", "limit",
    "join", "asc", "desc", "from", "on", "not"
  )

  lexical.delimiters += (
    "*", "+", "-", "<", "=", "<=", ">=", ">", "/", "(", ")", ",", "."
  )

  def select: Parser[SelectStmt] = 
    "select" ~> projections ~ 
      opt(relations) ~ opt(filter) ~ 
      opt(groupBy) ~ opt(orderBy) ~ opt(limit) ^^ {
    case p ~ r ~ f ~ g ~ o ~ l => SelectStmt(p, r, f, g, o, l)
  }

  def projections: Parser[Seq[SqlProj]] = repsep(projection, ", ")

  def projection: Parser[SqlProj] =
    "*" ^^^ (StarProj) |
    expr ~ opt("as" ~> ident) ^^ {
      case expr ~ ident => ExprProj(expr, ident)
    }

  def expr: Parser[SqlExpr] = or_expr

  def or_expr: Parser[SqlExpr] = 
    and_expr * ( "or" ^^^ { (a: SqlExpr, b: SqlExpr) => OrClause(a, b) } ) 

  def and_expr: Parser[SqlExpr] =
    eq_expr * ( "and" ^^^ { (a: SqlExpr, b: SqlExpr) => AndClause(a, b) } ) 

  def eq_expr: Parser[SqlExpr] =
    cmp_expr * (
      "=" ^^^ { (a: SqlExpr, b: SqlExpr) => EQClause(a, b) } |
      "<>" ^^^ { (a: SqlExpr, b: SqlExpr) => NEQClause(a, b) } )

  def cmp_expr: Parser[SqlExpr] =
    add_expr * (
      "<" ^^^ { (a: SqlExpr, b: SqlExpr) => LTClause(a, b) } |
      "<=" ^^^ { (a: SqlExpr, b: SqlExpr) => LEClause(a, b) } |
      ">" ^^^ { (a: SqlExpr, b: SqlExpr) => GTClause(a, b) } |
      ">=" ^^^ { (a: SqlExpr, b: SqlExpr) => GEClause(a, b) } )

  def add_expr: Parser[SqlExpr] =
    mult_expr * (
      "+" ^^^ { (a: SqlExpr, b: SqlExpr) => PlusClause(a, b) } |
      "-" ^^^ { (a: SqlExpr, b: SqlExpr) => MinusClause(a, b) } )

  def mult_expr: Parser[SqlExpr] =
    not_expr * (
      "*" ^^^ { (a: SqlExpr, b: SqlExpr) => MultClause(a, b) } |
      "/" ^^^ { (a: SqlExpr, b: SqlExpr) => DivClause(a, b) } ) 

  def not_expr: Parser[SqlExpr] =
    opt("not") ~ primary_expr ^^ {
      case n ~ e => n.map(_ => NotClause(e)).getOrElse(e)
    }

  def primary_expr: Parser[SqlExpr] =
    literal | 
    ident ~ opt( "." ~> ident  | "(" ~> repsep(expr, ",") <~ ")" ) ^^ {
      case id ~ None => FieldIdent(None, id)
      case a ~ Some( b: String ) => FieldIdent(Some(a), b)
      case a ~ Some( xs: Seq[_] ) => FunctionCall(a, xs.asInstanceOf[Seq[SqlExpr]])
    } |
    "(" ~> (expr | select ^^ (Subselect(_))) <~ ")" 

  def literal: Parser[SqlExpr] =
    numericLit ^^ { case i => IntLiteral(i.toInt) } |
    stringLit ^^ { case s => StringLiteral(stripQuotes(s)) }

  def relations: Parser[Seq[SqlRelation]] = "from" ~> rep1sep(relation, ",")

  def relation: Parser[SqlRelation] =
    simple_relation ~ rep("join" ~ simple_relation ~ "on" ~ expr ^^ { case "join" ~ r ~ "on" ~ e => (r, e)}) ^^ {
      case r ~ elems => elems.foldLeft(r) { case (x, r) => JoinRelation(x, r._1, r._2) }
    }

  def simple_relation: Parser[SqlRelation] = 
    ident ~ opt(ident) ^^ { 
      case ident ~ alias => TableRelation(ident, alias)
    } |
    select ~ "as" ~ ident ^^ {
      case select ~ "as" ~ alias => SubqueryRelation(select, alias) 
    }

  def filter: Parser[SqlExpr] = "where" ~> expr

  def groupBy: Parser[SqlGroupBy] = 
    "group" ~> "by" ~> repsep(ident, ",") ^^ (SqlGroupBy(_))

  def orderBy: Parser[SqlOrderBy] = 
    "order" ~> "by" ~> repsep( ident ~ opt("asc" | "desc") ^^ { 
      case i ~ (Some("asc") | None) => (i, ASC)
      case i ~ Some("desc") => (i, DESC)
    }, ",") ^^ (SqlOrderBy(_))

  // TODO: don't allow negative number
  def limit: Parser[Int] = "limit" ~> numericLit ^^ (_.toInt)

  private def stripQuotes(s:String) = s.substring(1, s.length-1)

  def parse(sql:String): Option[SelectStmt] = {
    phrase(select)(new lexical.Scanner(sql)) match {
      case Success(r, q) => Option(r)
      case x => println(x); None
    }
  }
}
