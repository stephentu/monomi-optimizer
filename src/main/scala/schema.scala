import java.sql._
import java.util._

abstract class Relation(val name: String, val columns: Seq[Column]) {
  def lookupColumn(name: String): Option[Column] = {
    columns filter (_.name == name) headOption
  }
}
case class TableRelation(override val name: String, override val columns: Seq[Column]) extends Relation(name, columns)
case class SubqueryRelation(override val name: String, override val columns: Seq[Column], stmt: SelectStmt) extends Relation(name, columns)

abstract class Column(val name: String, val tpe: DataType)
case class TableColumn(override val name: String, override val tpe: DataType, relation: String) extends Column(name, tpe)
case class AliasedColumn(override val name: String, orig: Column) extends Column(name, orig.tpe)
case class ExprColumn(override val name: String, expr: SqlExpr) extends Column(name, UnknownType)
case class VirtualColumn(expr: SqlExpr) extends Column("<virtual>", UnknownType) {
  assert(expr.getPrecomputableRelation.isDefined)
  val relation: String = expr.getPrecomputableRelation.get
}

trait Schema {
  def loadSchema(): Map[String, Relation]
}

class PgSchema(hostname: String, port: Int, db: String, props: Properties) {
  Class.forName("org.postgresql.Driver")
  private val conn = DriverManager.getConnection(
    "jdbc:postgresql://%s:%d/%s".format(hostname, port, db), props)

  def loadSchema() = {
    import Conversions._
    val s = conn.prepareStatement("""
select table_name from information_schema.tables 
where table_catalog = ? and table_schema = 'public'
      """)
    s.setString(1, db)
    val r = s.executeQuery    
    val tables = r.map(_.getString(1))
    s.close()

    tables.map(name => {
      val s = conn.prepareStatement("""
select 
  column_name, data_type, character_maximum_length, 
  numeric_precision, numeric_precision_radix, numeric_scale 
from information_schema.columns 
where table_schema = 'public' and table_name = ?
""")
      s.setString(1, name)
      val r = s.executeQuery
      val columns = r.map(rs => {
        val cname = rs.getString(1)
        TableColumn(cname, rs.getString(2) match {
          case "character varying" => VariableLenString(rs.getInt(3))
          case "character" => FixedLenString(rs.getInt(3))
          case "date" => DateType
          case "numeric" => DecimalType(rs.getInt(4), rs.getInt(6))
          case "integer" => 
            assert(rs.getInt(4) % 8 == 0)
            IntType(rs.getInt(4) / 8)
          case e => sys.error("unknown type: " + e)
        }, name)
      })
      s.close()
      (name, TableRelation(name, columns))
    }).toMap
  }
}
