import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._

class SQLParser extends StandardTokenParsers {

  val functions = Seq("count", "sum", "avg", "min", "max", "substring", "extract")

  lexical.reserved += (
    "select", "as", "or", "and", "group", "order", "by", "where", "limit",
    "join", "asc", "desc", "from", "on", "not", "having", "distinct",
    "case", "for", "from", "exists", "between", "like", "in", 
    "year", "month", "day", "null", "is"
  )
  
  lexical.reserved ++= functions

  lexical.delimiters += (
    "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", "."
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
    and_expr * ( "or" ^^^ { (a: SqlExpr, b: SqlExpr) => Or(a, b) } ) 

  def and_expr: Parser[SqlExpr] =
    cmp_expr * ( "and" ^^^ { (a: SqlExpr, b: SqlExpr) => And(a, b) } ) 

  // TODO: this function is nasty- clean it up!
  def cmp_expr: Parser[SqlExpr] =
    add_expr ~ rep(
      ("=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") ~ add_expr ^^ {
        case op ~ rhs => (op, rhs)
      } |
      "between" ~ add_expr ~ "and" ~ add_expr ^^ {
        case op ~ a ~ _ ~ b => (op, a, b) 
      } |
      "in" ~ "(" ~ (select | rep1sep(expr, ",")) ~ ")" ^^ {
        case op ~ _ ~ a ~ _ => (op, a)
      } |
      "like" ~ add_expr ^^ { case op ~ a => (op, a) }
    ) ^^ {
      case lhs ~ elems =>
        elems.foldLeft(lhs) {
          case (acc, (("=", rhs: SqlExpr))) => Eq(acc, rhs)
          case (acc, (("<>", rhs: SqlExpr))) => Neq(acc, rhs)
          case (acc, (("!=", rhs: SqlExpr))) => Neq(acc, rhs)
          case (acc, (("<", rhs: SqlExpr))) => Lt(acc, rhs)
          case (acc, (("<=", rhs: SqlExpr))) => Le(acc, rhs)
          case (acc, ((">", rhs: SqlExpr))) => Gt(acc, rhs)
          case (acc, ((">=", rhs: SqlExpr))) => Ge(acc, rhs)
          case (acc, (("between", l: SqlExpr, r: SqlExpr))) => And(Ge(acc, l), Le(acc, r))
          case (acc, (("in", e: Seq[_]))) => In(acc, Left(e.asInstanceOf[Seq[SqlExpr]]))
          case (acc, (("in", s: SelectStmt))) => In(acc, Right(s))
          case (acc, (("like", e: SqlExpr))) => Like(acc, e)
        }
    } |
    "not" ~> cmp_expr ^^ (Not(_)) |
    "exists" ~> "(" ~> select <~ ")" ^^ (Exists(_))

  def add_expr: Parser[SqlExpr] =
    mult_expr * (
      "+" ^^^ { (a: SqlExpr, b: SqlExpr) => Plus(a, b) } |
      "-" ^^^ { (a: SqlExpr, b: SqlExpr) => Minus(a, b) } )

  def mult_expr: Parser[SqlExpr] =
    primary_expr * (
      "*" ^^^ { (a: SqlExpr, b: SqlExpr) => Mult(a, b) } |
      "/" ^^^ { (a: SqlExpr, b: SqlExpr) => Div(a, b) } ) 

  def primary_expr: Parser[SqlExpr] =
    literal | 
    known_function |
    ident ~ opt( "." ~> ident  | "(" ~> repsep(expr, ",") <~ ")" ) ^^ {
      case id ~ None => FieldIdent(None, id)
      case a ~ Some( b: String ) => FieldIdent(Some(a), b)
      case a ~ Some( xs: Seq[_] ) => FunctionCall(a, xs.asInstanceOf[Seq[SqlExpr]])
    } |
    "(" ~> (expr | select ^^ (Subselect(_))) <~ ")" 

  def known_function: Parser[SqlExpr] =
    "count" ~> "(" ~> ( "*" ^^^ (CountStar) | opt("distinct") ~ expr ^^ { case d ~ e => CountExpr(e, d.isDefined) }) <~ ")" |
    "min" ~> "(" ~> expr <~ ")" ^^ (Min(_)) |
    "max" ~> "(" ~> expr <~ ")" ^^ (Max(_)) |
    "sum" ~> "(" ~> (opt("distinct") ~ expr) <~ ")" ^^ { case d ~ e => Sum(e, d.isDefined) } |
    "avg" ~> "(" ~> (opt("distinct") ~ expr) <~ ")" ^^ { case d ~ e => Avg(e, d.isDefined) } |
    "extract" ~> ("year" | "month" | "day") ~ "from" ~ expr ^^ {
      case "year" ~ "from" ~ e => Extract(e, YEAR)
      case "month" ~ "from" ~ e => Extract(e, MONTH)
      case "day" ~ "from" ~ e => Extract(e, DAY)
    } |
    "substring" ~> "(" ~> ( expr ~ "from" ~ numericLit ~ opt("for" ~> numericLit) ) <~ ")" ^^ {
      case e ~ "from" ~ a ~ b => Substring(e, a.toInt, b.map(_.toInt))
    }

  def literal: Parser[SqlExpr] =
    numericLit ^^ { case i => IntLiteral(i.toInt) } |
    stringLit ^^ { case s => StringLiteral(stripQuotes(s)) }
    "null" ^^^ (NullLiteral)

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
    "group" ~> "by" ~> repsep(ident, ",") ~ opt("having" ~> expr) ^^ {
      case k ~ h => SqlGroupBy(k, h)
    }

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
