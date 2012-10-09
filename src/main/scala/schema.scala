package edu.mit.cryptdb

import scala.collection.mutable.{ ArrayBuffer, HashMap }

import java.sql.{ Array => SqlArray, _ }
import java.util.Properties

// these are the types of relations which can show up in a
// FROM clause
abstract trait Relation
case class TableRelation(name: String) extends Relation
case class SubqueryRelation(stmt: SelectStmt) extends Relation

case class TableColumn(name: String, tpe: DataType, partOfPK: Boolean) extends PrettyPrinters {
  def scalaStr: String =
    "TableColumn(" + quoteDbl(name) + ", " + tpe.toString + ", " + partOfPK + ")"
}

class Definitions(val defns: Map[String, Seq[TableColumn]], val dbconn: Option[DbConn])
  extends PrettyPrinters {

  def tableExists(table: String): Boolean = defns.contains(table)

  def lookup(table: String, col: String): Option[TableColumn] = {
    defns.get(table).flatMap(_.filter(_.name == col).headOption)
  }

  def lookupPosition(table: String, col: String): Option[Int] = {
    defns.get(table).flatMap(_.zipWithIndex.filter(_._1.name == col).map(_._2).headOption)
  }

  def lookupPartOfPK(table: String, col: String): Option[Boolean] = {
    defns.get(table).flatMap(_.filter(_.name == col).headOption).map(_.partOfPK)
  }

  def scalaStr: String = {
    "new Definitions(Map(" + (defns.map { case (k, v) =>
      (quoteDbl(k), "Seq(" + v.map(_.scalaStr).mkString(", ") + ")")
    }.map { case (k, v) => k + " -> " + v }.mkString(", ")) + "), None)"
  }

  def lookupByColumnName(col: String): Seq[(String, TableColumn)] = {
    defns.toSeq.flatMap { case (r, v) => v.filter(_.name == col).map(t => (r, t)) }
  }
}

case class ColumnStats(
  null_frac: Double,
  n_distinct: Double,
  correlation: Double,
  most_common_vals: Option[Seq[_]],
  most_common_freqs: Option[Seq[Double]],
  histogram_bounds: Option[Seq[_]])

case class TableStats(
  row_count: Long,
  column_stats: Map[String, ColumnStats])

object Statistics {
  final val empty = new Statistics(Map.empty, None)
}

class Statistics(val stats: Map[String, TableStats], val dbconn: Option[DbConn])

trait DbConn extends Timer {
  def getConn: Connection

  def db: String

  private val _tblCache =
    new java.util.concurrent.ConcurrentHashMap[(Connection, Long), String]

  def makeIntTemporaryTable(nRows: Long, nameGenerator: => String): (Boolean, String) = {
    val conn = getConn
    var ret = _tblCache.get((conn, nRows))
    var created = false
    if (ret eq null) {
      synchronized {
        // DCL-ed
        ret = _tblCache.get((conn, nRows))
        if (ret eq null) {
          // creator

          val tempTableName = nameGenerator
          val stmt = conn.createStatement
          stmt.executeUpdate("CREATE TEMPORARY TABLE %s ( col0 INTEGER )".format(tempTableName))

          val insertTime =
            timedRunMillisNoReturn(
              stmt.executeUpdate("INSERT INTO %s VALUES %s".format(
                tempTableName,
                (1L to nRows).map(x => "(%d)".format(x * 2)).mkString(","))))

          println("insert time for temp table %s is %f ms".format(tempTableName, insertTime))

          val analyzeTime =
            timedRunMillisNoReturn(
              stmt.executeUpdate("ANALYZE %s".format(tempTableName)))

          println("analyze time for temp table %s is %f ms".format(tempTableName, analyzeTime))

          stmt.close()

          created = true

          val old = _tblCache.put((conn, nRows), tempTableName)
          assert(old eq null)

          ret = tempTableName
        }
      }
    }
    (created, ret)
  }
}

class PgDbConn(val hostname: String, val port: Int, val db: String, val props: Properties) extends DbConn {
  Class.forName("org.postgresql.Driver")

  // XXX: poor man's connection pooling...
  // unfortunately, these connections never get closed
  private val _connTL = new ThreadLocal[Connection] {
    override protected def initialValue(): Connection = {
      DriverManager.getConnection(
        "jdbc:postgresql://%s:%d/%s".format(hostname, port, db), props)
    }
  }

  def getConn = _connTL.get()
}

trait Schema {
  def loadSchema(): Definitions
  def loadStats(): Statistics
}

class PgDbConnSchema(_dbconn: PgDbConn) extends Schema with PgQueryPlanExtractor {

  private val conn = _dbconn.getConn

  import Conversions._

  private def listTables(): Seq[String] = {
    val s = conn.prepareStatement("""
select table_name from information_schema.tables
where table_catalog = ? and (table_schema = 'public' or table_schema like 'pg_temp_%')
      """)
    s.setString(1, _dbconn.db)
    val r = s.executeQuery
    val tables = r.map(_.getString(1))
    s.close()
    tables
  }

  def loadSchema() = {
    val tables = listTables()

    val ret = new Definitions(tables.map(name => {

      val s1 = conn.prepareStatement("""
  select
      a.attname as column_name
  from
      information_schema.table_constraints tc,
      pg_class t,
      pg_class i,
      pg_index ix,
      pg_attribute a
  where
      tc.table_name = ?
      and tc.constraint_type = 'PRIMARY KEY'
      and t.oid = ix.indrelid
      and i.oid = ix.indexrelid
      and a.attrelid = t.oid
      and a.attnum = ANY(ix.indkey)
      and i.relname = tc.constraint_name
  """)

      s1.setString(1, name)
      val r1 = s1.executeQuery
      val pkCols = r1.map(_.getString(1)).toSet
      s1.close()

      val s = conn.prepareStatement("""
  select
  column_name, data_type, character_maximum_length,
  numeric_precision, numeric_precision_radix, numeric_scale
  from information_schema.columns
  where (table_schema = 'public' or table_schema like 'pg_temp_%') and table_name = ?
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
          case "bigint" => IntType(8)
          case "smallint" => IntType(2)
          case "bytea" => VariableLenByteArray(None)
          case e => sys.error("unknown type: " + e)
        }, pkCols.contains(cname))
      })
      s.close()

      (name, columns.reverse) // TODO: not sure why the result set is coming in
                              // reverse order, but not really interested in
                              // debugging it now
    }).toMap, Some(_dbconn))

    ret
  }

  import org.postgresql.util.PGobject

  private trait ElemBuilder[T] {
    def mkFromString(s: String): T
  }

  private implicit object StringElemBuilder extends ElemBuilder[String] {
    def mkFromString(s: String) = s
  }

  private implicit object IntElemBuilder extends ElemBuilder[Int] {
    def mkFromString(s: String) = s.toInt
  }

  private implicit object LongElemBuilder extends ElemBuilder[Long] {
    def mkFromString(s: String) = s.toLong
  }

  private implicit object DoubleElemBuilder extends ElemBuilder[Double] {
    def mkFromString(s: String) = s.toDouble
  }

  private implicit object DateElemBuilder extends ElemBuilder[java.util.Date] {
    import java.text._
    private val df = new SimpleDateFormat("yyyy-mm-dd")
    def mkFromString(s: String) = df.parse(s, new ParsePosition(0))
  }

  private def seqFromPGAnyArray[T](o: PGobject)(implicit t: ElemBuilder[T]): Seq[T] = {
    assert(o.getType == "anyarray")

    sealed abstract trait PState
    case object BeginBrace extends PState
    case object ElemBegin extends PState
    case object InRegElem extends PState
    case object InQuotedElem extends PState
    case object ElemEnd extends PState

    val payload = o.getValue
    var idx = 0
    var state: PState = BeginBrace

    def expect(c: Char, s: PState) = {
      val c0 = payload(idx)
      if (c0 != c) {
        throw new RuntimeException("expecting " + c + ", got " + c0)
      }
      idx += 1
      state = s
    }

    val res = new ArrayBuffer[T]
    var buf = new StringBuilder

    while (idx < payload.length) {
      state match {
        case BeginBrace =>
          expect('{', ElemBegin)

        case ElemBegin =>
          buf = new StringBuilder
          val c = payload(idx)
          if (c == '"') {
            idx += 1
            state = InQuotedElem
          } else if (c == '}') {
            idx += 1
            state = ElemEnd
          } else {
            buf.append(c)
            idx += 1
            state = InRegElem
          }

        case InRegElem =>
          val c = payload(idx)
          if (c == ',') {
            res += t.mkFromString(buf.toString)
            idx += 1
            state = ElemBegin
          } else if (c == '}') {
            res += t.mkFromString(buf.toString)
            idx += 1
            state = ElemEnd
          } else {
            buf.append(c)
            idx += 1
          }

        case InQuotedElem =>
          val c = payload(idx)
          if (c == '\\') {
            buf.append(payload(idx + 1))
            idx += 2
          } else if (c == '"') {
            res += t.mkFromString(buf.toString)
            idx += 1

            val c0 = payload(idx)
            if (c0 == ',') {
              idx += 1
              state = ElemBegin
            } else if (c0 == '}') {
              idx += 1
              state = ElemEnd
            } else {
              assert(false)
            }

          } else {
            buf.append(c)
            idx += 1
          }

        case ElemEnd =>
          throw new RuntimeException("should not have elems past end of array")
      }
    }

    if (state != ElemEnd) {
      println("state = " + state)
      println(" on input: " + payload)
      throw new RuntimeException("bad parse")
    }

    res.toSeq
  }

  def loadStats() = {
    val schema = loadSchema()
    // TODO: this currently doesn't work for temp tables
    val s = conn.prepareStatement("""
select
  attname, null_frac, n_distinct, correlation,
  most_common_vals, most_common_freqs, histogram_bounds
from pg_stats
where schemaname = 'public' and tablename = ?
""")

    val ret = new Statistics(
      schema.defns.map { case (name, cols) =>
        // get an estimate of the row count
        val (_, rows, _, _, _) = extractCostFromDBSql("SELECT * FROM %s".format(name), _dbconn)
        val typeMap = cols.map(tc => (tc.name, tc.tpe)).toMap
        s.setString(1, name)
        val r = s.executeQuery
        val cmap = r.map { rs =>
          val attname = rs.getString(1)
          val null_frac = rs.getDouble(2)
          val n_distinct = rs.getDouble(3)
          val correlation = rs.getDouble(4)

          def mkSeq(pg: PGobject, tpe: DataType): Seq[_] =
            tpe match {
              case IntType(i) if i <= 4 => seqFromPGAnyArray[Int](pg)
              case IntType(i) => seqFromPGAnyArray[Long](pg)
              case _: DecimalType => seqFromPGAnyArray[Double](pg)
              case _: FixedLenString => seqFromPGAnyArray[String](pg)
              case _: VariableLenString => seqFromPGAnyArray[String](pg)
              case DateType => seqFromPGAnyArray[java.util.Date](pg)
              case _ => throw new RuntimeException("cannot handle: " + tpe)
            }

          val most_common_vals = Option(rs.getObject(5).asInstanceOf[PGobject]).map(pg => mkSeq(pg, typeMap(attname)))
          val most_common_freqs = Option(rs.getArray(6)).map(_.getArray.asInstanceOf[Array[java.lang.Float]].map(_.toDouble).toSeq)
          val histogram_bounds = Option(rs.getObject(7).asInstanceOf[PGobject]).map(pg => mkSeq(pg, typeMap(attname)))

          (attname, ColumnStats(null_frac, n_distinct, correlation,
                                most_common_vals, most_common_freqs,
                                histogram_bounds))
        }.toMap

        (name, TableStats(rows, cmap))
      }.toMap, Some(_dbconn))
    s.close()
    ret
  }
}

class PgSchema(hostname: String, port: Int, db: String, props: Properties)
  extends PgDbConnSchema(new PgDbConn(hostname, port, db, props))
