package edu.mit.cryptdb

import scala.util.parsing.json._
import scala.collection.mutable.{ ArrayBuffer, HashMap }

object GlobalOpts {
  final val empty = GlobalOpts(Map.empty, Map.empty)
}

case class GlobalOpts(
  precomputed: Map[String, Map[String, SqlExpr]],
  homGroups: Map[String, Seq[Seq[SqlExpr]]]) {

  def merge(that: GlobalOpts): GlobalOpts = {

    def mergeA(lhs: Map[String, Map[String, SqlExpr]],
               rhs: Map[String, Map[String, SqlExpr]]):
      Map[String, Map[String, SqlExpr]] = {
      (lhs.keys ++ rhs.keys).map { k =>
        (k, lhs.getOrElse(k, Map.empty) ++ rhs.getOrElse(k, Map.empty))
      }.toMap
    }

    def mergeB(lhs: Map[String, Seq[Seq[SqlExpr]]],
               rhs: Map[String, Seq[Seq[SqlExpr]]]):
      Map[String, Seq[Seq[SqlExpr]]] = {
      (lhs.keys ++ rhs.keys).map { k =>
        (k, (lhs.getOrElse(k, Seq.empty).toSet ++ rhs.getOrElse(k, Seq.empty).toSet).toSeq)
      }.toMap
    }

    GlobalOpts(
      mergeA(precomputed, that.precomputed),
      mergeB(homGroups, that.homGroups))
  }

}

case class EstimateContext(
  defns: Definitions,

  globalOpts: GlobalOpts,

  // the onions required for the plan to work
  requiredOnions: Map[String, Map[String, Int]],

  // the pre-computed exprs (w/ the req onion) required for the plan to work
  precomputed: Map[String, Map[String, Int]],

  // the hom groups required for the plan to work
  // set(int) references the global opt's homGroup
  homGroups: Map[String, Set[Int]]) {

  private val _idGen = new NameGenerator("fresh_")
  @inline def uniqueId(): String = _idGen.uniqueId()
}

case class Estimate(
  cost: Double,
  rows: Long,
  rowsPerRow: Long /* estimate cardinality within each aggregate group */,
  equivStmt: SelectStmt /* statement which estimates the equivalent CARDINALITY */,
  seqScanInfo: Map[String, Int] /* the number of times a seq scan is invoked, per relation */) {

  override def toString = {
    case class Estimate(c: Double, r: Long, rr: Long, ssi: Map[String, Int])
    Estimate(cost, rows, rowsPerRow, seqScanInfo).toString
  }
}

object CostConstants {

  final val PGUnitPerSec: Double = 85000.0

  final val NetworkXferBytesPerSec: Double = 10485760.0 // 10 MB/sec

  final val DiskReadBytesPerSec: Double = 104857600.0 // 100 MB/sec

//DET_ENC    = 0.0151  / 1000.0
//DET_DEC    = 0.0173  / 1000.0
//OPE_ENC    = 13.359  / 1000.0
//OPE_DEC    = 9.475   / 1000.0
//AGG_DEC    = 0.6982  / 1000.0
//AGG_ADD    = 0.00523 / 1000.0
//SWP_ENC    = 0.00373 / 1000.0
//SWP_SEARCH = 0.00352 / 1000.0

  final val AggAddSecPerOp: Double    = 0.00523 / 1000.0
  final val SwpSearchSecPerOp: Double = 0.00352 / 1000.0

  // encrypt map (cost in seconds)
  final val EncryptCostMap: Map[Int, Double] =
    Map(
      Onions.DET -> (0.0151  / 1000.0),
      Onions.OPE -> (13.359  / 1000.0),
      Onions.SWP -> (0.00373 / 1000.0))

  // decrypt map (cost in seconds)
  final val DecryptCostMap: Map[Int, Double] =
    Map(
      Onions.DET -> (0.0173  / 1000.0),
      Onions.OPE -> (9.475   / 1000.0),
      Onions.HOM -> (0.6982  / 1000.0))

  @inline def secToPGUnit(s: Double): Double = {
    s * PGUnitPerSec
  }
}

case class PosDesc(
  origTpe: DataType, 
  origFieldname: Option[(String, String)],
  onion: OnionType, 
  vectorCtx: Boolean) {

  def toCPP: String = {
    "db_column_desc(%s, %s, %s, %s, %b)".format(
      origTpe.toCPP,
      onion.toCPP,
      onion.seclevelToCPP,
      origFieldname.map(_._2).getOrElse(""),
      vectorCtx)
  }
}

case class UserAggDesc(
  rows: Long,
  rowsPerRow: Option[Long],
  selectivityMap: Map[String, Long])

trait PgQueryPlanExtractor {

  // the 4th return value is a map of
  //
  def extractCostFromDBStmt(stmt: SelectStmt, dbconn: DbConn,
                            stats: Option[Statistics] = None):
    (Double, Long, Option[Long],
     Map[String, UserAggDesc], Map[String, Int]) = {
    extractCostFromDBSql(stmt.sqlFromDialect(PostgresDialect), dbconn, stats)
  }

  def extractCostFromDBSql(sql: String, dbconn: DbConn,
                           stats: Option[Statistics] = None):
    (Double, Long, Option[Long],
     Map[String, UserAggDesc], Map[String, Int]) = {
    // taken from:
    // http://stackoverflow.com/questions/4170949/how-to-parse-json-in-scala-using-standard-scala-classes
    class CC[T] {
      def unapply(a: Any): Option[T] = Some(a.asInstanceOf[T])
    }
    object M extends CC[Map[String, Any]]
    object L extends CC[List[Any]]

    object S extends CC[String]
    object D extends CC[Double]
    object B extends CC[Boolean]

    type ExtractInfo =
      (Double, Long, Option[Long],
       Map[String, UserAggDesc], Map[String, Long],
       Map[String, Int])

    def extractInfoFromQueryPlan(node: Map[String, Any]): ExtractInfo = {
      val childrenNodes =
        for (L(children) <- node.get("Plans").toList; M(child) <- children)
        yield (child("Parent Relationship").asInstanceOf[String], extractInfoFromQueryPlan(child))

      val outerChildren = childrenNodes.filter(_._1 == "Outer").map(_._2)
      val innerOuterChildren = childrenNodes.filter(x => x._1 == "Outer" || x._1 == "Inner").map(_._2)

      val Some((totalCost, planRows)) = for (
        D(totalCost) <- node.get("Total Cost");
        D(planRows)  <- node.get("Plan Rows")
      ) yield ((totalCost, planRows.toLong))

      def noOverwriteFoldLeft[K, V](ss: Seq[Map[K, V]]): Map[K, V] = {
        ss.foldLeft( Map.empty : Map[K, V] ) {
          case (acc, m) =>
            acc ++ m.flatMap { case (k, v) =>
              if (!acc.contains(k)) Some((k, v)) else None
            }.toMap
        }
      }

      def mergePlus(lhs: Map[String, Int], rhs: Map[String, Int]): Map[String, Int] = {
        (lhs.keys ++ rhs.keys).map { k =>
          (k, lhs.getOrElse(k, 0) + rhs.getOrElse(k, 0))
        }.toMap
      }

      val userAggMapFold = noOverwriteFoldLeft(childrenNodes.map(_._2).map(_._4).toSeq)
      val selMapFold = noOverwriteFoldLeft(innerOuterChildren.map(_._5).toSeq)
      val seqScanInfoFold = childrenNodes.foldLeft(Map.empty : Map[String, Int]) {
        case (acc, est) =>
          mergePlus(acc, est._2._6)
      }

      val tpe = node("Node Type")

      tpe match {
        case "Aggregate" =>
          // must have outer child
          assert(!outerChildren.isEmpty)

          // compute the values that the aggs will have to experience, based
          // on the first outer child
          val (rOuter, rrOuter) =
            outerChildren.head match { case (_, r, rr, _, _, _) => (r, rr) }

          // see if it has a hom_agg aggregate
          // TODO: this is quite rigid.. we should REALLY be using our
          // SQL parser to parse this expr, except it doesn't support the
          // postgres extension ::cast syntax...
          val HomAggRegex = "hom_agg\\(.*, '[a-zA-Z_]+'::character varying, \\d+, '([a-zA-Z0-9_]+)'::character varying\\)".r
          val aggs =
            (for (L(fields) <- node.get("Output").toList;
                  S(expr)   <- fields) yield {
              expr match {
                case HomAggRegex(id) =>
                  Some((id, UserAggDesc(rOuter, rrOuter, selMapFold)))
                case _ => None
              }
            }).flatten.toMap

          // compute the values of THIS node based on the first outer child
          val (r, rr) = outerChildren.head match {
            case (_, rows, None, _, _, _) =>
               (planRows, Some(math.ceil(rows.toDouble / planRows.toDouble).toLong))

            case (_, rows, Some(rowsPerRow), _, _, _) =>
               (planRows, Some(math.ceil(rows.toDouble / planRows.toDouble * rowsPerRow).toLong))
          }

          (totalCost, r, rr,
           noOverwriteFoldLeft(Seq(userAggMapFold, aggs)),
           selMapFold, seqScanInfoFold)

        case "Seq Scan" | "Index Scan" =>
          assert(innerOuterChildren.isEmpty) // seq scans don't have children?

          val S(reln) = node("Relation Name")

          // see if we have searchswp udf
          // once again, this is fragile, use SQL parser in future
          val SearchSWPRegex =
            "searchswp\\([a-zA-Z0-9_.]+, '[^']*'::character varying, NULL::character varying, '([a-zA-Z0-9_]+)'::character varying\\)".r

          val filterName = if (tpe == "Seq Scan") "Filter" else "Index Cond"

          val m = node.get("Filter").map { case S(f) =>
            SearchSWPRegex.findAllIn(f).matchData.map { m =>
              // need to estimate r from the relation
              val r = stats.map(_.stats(reln).row_count).getOrElse(
                extractCostFromDBSql("SELECT * FROM %s".format(reln), dbconn, None)._2)
              val id = m.group(1)
              ((id, UserAggDesc(r, None, Map.empty)))
            }.toMap
          }.getOrElse(Map.empty)

          val (r, rr) = (planRows, None)

          (totalCost, r, rr,
           noOverwriteFoldLeft(Seq(userAggMapFold, m)),
           Map(reln -> planRows), mergePlus(seqScanInfoFold, Map(reln -> 1)))

        case t =>
          // simple case, just read from this node only
          (totalCost, planRows, outerChildren.headOption.flatMap(_._3),
           userAggMapFold, selMapFold, seqScanInfoFold)
      }
    }

    import Conversions._

    val r =
      try {
        dbconn.getConn.createStatement.executeQuery("EXPLAIN (VERBOSE, FORMAT JSON) " + sql)
      } catch {
        case e =>
          println("bad sql:")
          println(sql)
          throw e
      }
    val res = r.map { rs =>
      val planJson =
        // JSON parser is not thread safe
        JSON.synchronized { JSON.parseFull(rs.getString(1)) }
      (for (L(l) <- planJson;
            M(m) = l.head;
            M(p) <- m.get("Plan")) yield extractInfoFromQueryPlan(p)).getOrElse(
        throw new RuntimeException("unexpected return from postgres: " + planJson)
      )
    }

    (res.head._1, res.head._2, res.head._3, res.head._4, res.head._6)
  }
}

trait PlanNode extends Traversals with Transformers with Resolver with PgQueryPlanExtractor {
  // actual useful stuff

  def tupleDesc: Seq[PosDesc]

  def costEstimate(ctx: EstimateContext, stats: Statistics): Estimate

  protected def resolveCheck(stmt: SelectStmt, defns: Definitions, force: Boolean = false):
    SelectStmt = {
    // this is used to debug whether or not our sql statements sent
    // to the DB for cardinality estimation are valid or not

    resolve(stmt, defns)
  }

  def emitCPPHelpers(cg: CodeGenerator): Unit

  def emitCPP(cg: CodeGenerator): Unit

  // printing stuff
  def pretty: String = pretty0(0)

  protected def pretty0(lvl: Int): String
  protected def childPretty(lvl: Int, child: PlanNode): String =
    endl + indent(lvl + 1) + child.pretty0(lvl + 1)

  protected def indent(lvl: Int) = " " * (lvl * 4)
  protected def endl: String = "\n"
}

// a temporary hack that should not need to exist in
// a properly designed system
object FieldNameHelpers {
  def basename(s: String): String = {
    if (s.contains("$")) s.split("\\$").dropRight(1).mkString("$")
    else s
  }

  // given a field name formatted name$ONION, returns the onion type
  // (as an int)
  def encType(s: String): Option[Int] = {
    if (!s.contains("$")) None
    else Onions.onionFromStr(s.split("\\$").last)
  }
}

case class RemoteSql(stmt: SelectStmt,
                     projs: Seq[PosDesc],
                     subrelations: Seq[(RemoteMaterialize, SelectStmt)] = Seq.empty)
  extends PlanNode with Transformers with PrettyPrinters {

  assert(stmt.projections.size == projs.size)
  def tupleDesc = projs
  def pretty0(lvl: Int) = {
    "* RemoteSql(sql = " + stmt.sql + ", projs = " + projs + ")" +
    subrelations.map(c => childPretty(lvl, c._1)).mkString("")
  }

  def costEstimate(ctx: EstimateContext, stats: Statistics) = {
    // TODO: this is very hacky, and definitely prone to error
    // but it's a lot of mindless work to propagate the precise
    // node replacement information, so we just use some text
    // substitution for now, and wait to implement a real solution

    import FieldNameHelpers._

    def rewriteWithQual[N <: Node](n: N, q: String): N =
      topDownTransformation(n) {
        case f: FieldIdent => (Some(f.copy(qualifier = Some(q))), false)
        case _ => (None, true)
      }.asInstanceOf[N]

    val reverseStmt = topDownTransformation(stmt) {
      case a @ AggCall("hom_agg", args, _) =>
        // need to assign a unique ID to this hom agg
        (Some(a.copy(args = args ++ Seq(StringLiteral(ctx.uniqueId())))), true)

      case s @ FunctionCall("searchSWP", args, _) =>
        // need to assign a unique ID to this UDF 
        (Some(s.copy(args = args ++ Seq(NullLiteral(), StringLiteral(ctx.uniqueId())))), true)

      case FieldIdent(Some(qual), name, _, _) =>
        // check precomputed first
        val qual0 = basename(qual)
        val name0 = basename(name)
        ctx.globalOpts.precomputed.get(qual0).flatMap(_.get(name0))
          .map(x => (Some(rewriteWithQual(x, qual0)), false))
          .getOrElse {
            // rowids are rewritten to 0
            if (ctx.homGroups.contains(qual0) && name0 == "rowid") {
              ((Some(IntLiteral(0)), false))
            } else if (qual != qual0 || name != name0) {
              ((Some(FieldIdent(Some(qual0), name0)), false))
            } else ((None, false))
          }

      case TableRelationAST(name, alias, _) =>
        val SRegex = "subrelation\\$(\\d+)".r
        name match {
          case SRegex(srpos) =>
            // need to replace with SubqueryRelationAST
            (Some(SubqueryRelationAST(subrelations(srpos.toInt)._2, alias.get)), false)
          case _ =>
            val name0 = basename(name)
            if (ctx.defns.tableExists(name0)) {
              (Some(TableRelationAST(name0, alias)), false)
            } else {
              (None, false)
            }
        }

      case FunctionCall("encrypt", Seq(e, _, _), _) =>
        (Some(e), false)

      case _: BoundDependentFieldPlaceholder =>
        // TODO: we should generate something smarter than this
        // - at least the # should be a reasonable number for what it is
        // - replacing
        (Some(IntLiteral(12345)), false)

      case FunctionCall("hom_row_desc_lit", Seq(e), _) =>
        (Some(e), false)

      case _ => (None, true)
    }.asInstanceOf[SelectStmt]

    val reverseStmt0 = resolveCheck(reverseStmt, ctx.defns, true)

    // server query execution cost
    val (c, r, rr, m, ssi) =
      extractCostFromDBStmt(reverseStmt0, ctx.defns.dbconn.get, Some(stats))

    //println("sql: " + reverseStmt.sqlFromDialect(PostgresDialect))
    //println("m: " + m)

    var aggCost: Double = 0.0

    def topDownTraverseCtx(stmt: SelectStmt): Unit = {
      topDownTraversal(stmt) {

        case f @ FunctionCall("searchSWP", args, _) =>
          val Seq(_, _, _, StringLiteral(id, _)) = args
          aggCost += m(id).rows * CostConstants.secToPGUnit(CostConstants.SwpSearchSecPerOp)
          false

        case a @ AggCall("hom_agg", args, sqlCtx) =>
          val Seq(_, StringLiteral(tbl, _), IntLiteral(grp, _), StringLiteral(id, _)) = args
          assert(sqlCtx == stmt.ctx)

          // compute correlation score
          val corrScore =
            stmt.groupBy.map { case SqlGroupBy(keys, _, _) =>
              // multiply the correlation scores of each key
              //
              // if expr is not a key but rather an expression,
              // just assume key correlation score of 0.33
              //
              // the meaning of the correlation number is given in:
              // http://www.postgresql.org/docs/9.1/static/view-pg-stats.html
              //
              // since the value given is between +/- 1.0, we simply take the
              // abs value

              def followProjs(e: SqlExpr): SqlExpr =
                e match {
                  case FieldIdent(_, _, ProjectionSymbol(name, ctx, _), _) =>
                    val expr1 = ctx.lookupProjection(name).get
                    followProjs(expr1)
                  case _ => e
                }

              val DefaultGuess = 0.333

              keys.foldLeft(1.0) {
                case (acc, expr) =>
                  val corr = followProjs(expr) match {
                    case FieldIdent(_, _, ColumnSymbol(reln, col, _, _), _) =>
                      stats.stats.get(reln)
                        .flatMap(_.column_stats.get(col).map(x => math.abs(x.correlation)))
                        .getOrElse(DefaultGuess)
                    case _ => DefaultGuess // see above note
                  }
                  acc * corr
              }
            }.getOrElse(1.0)

          // find selectivity of the table. compute as a fraction of the
          // # of rows of the table
          val selScore =
            m(id).selectivityMap.get(tbl).map { rows =>
              val sel = rows.toDouble / stats.stats(tbl).row_count.toDouble
              assert(sel >= 0.0)
              assert(sel <= 1.0)
              sel
            }.getOrElse {
              println("Could not compute selectivity info for table %s, assuming 0.75".format(tbl))
              0.75
            }

          // totalScore
          val totalScore = corrScore * selScore
          assert(totalScore >= 0.0)
          assert(totalScore <= 1.0)

          // TODO: need to take into account ciphertext size
          val costPerAgg = CostConstants.secToPGUnit(CostConstants.AggAddSecPerOp)

          val homGroup = ctx.globalOpts.homGroups(tbl)(grp.toInt)
          // assume each group right now take 83 bits of ciphertext space

          val sizePerGroupElem = 83 // assumes that we do perfect packing (size in PT)

          val minAggPTSize = 1024 // bits in PT

          val fileSize = // bytes
            math.max(
              (stats.stats(tbl).row_count * homGroup.size * sizePerGroupElem * 2).toDouble / 8.0,
              minAggPTSize.toDouble * 2.0 / 8.0 /* min size (kind of a degenerate case */)

          // for now, assume that we only have to read proportional to the selectivity factor
          // of the table (this is optimistic)
          val readTime = (fileSize * selScore) / CostConstants.DiskReadBytesPerSec

          val readCost = CostConstants.secToPGUnit(readTime)

          val packingFactor =
            math.max(
              math.ceil( minAggPTSize.toDouble / (sizePerGroupElem * homGroup.size).toDouble ),
              1.0)

          assert(packingFactor >= 1.0)

          // min cost of agg is perfect packing
          val minCost =
            costPerAgg * m(id).rows.toDouble * m(id).rowsPerRow.getOrElse(1L).toDouble / packingFactor

          // max cost of agg is eval all rows separately
          val maxCost =
            costPerAgg * m(id).rows.toDouble * m(id).rowsPerRow.getOrElse(1L).toDouble

          // use totalScore to scale us between min/max cost
          val scaledCost = math.min(minCost / totalScore, maxCost)

          //println("call " + a.sql + " stats: " + m(id))

          // add to total cost
          aggCost += (readCost + scaledCost)

          false
        case SubqueryRelationAST(ss, _, _) =>
          topDownTraverseCtx(ss)
          false
        case Subselect(ss, _) =>
          topDownTraverseCtx(ss)
          false
        case e =>
          assert(e.ctx == stmt.ctx)
          true
      }
    }

    topDownTraverseCtx(reverseStmt0)

    // adjust sequential scans to reflect extra onions
    // TODO: we need to take onion sizes into account- for now, let's
    // just assume all onions are the same size. So the cost we get
    // from the DB includes 1 onion per column. We scan through the
    // required+precomputed onions and any columns which require > 1
    // onion get an additional cost tacked on to their sequential scans
    var addSeqScanCost = 0.0
    ssi.foreach { case (reln, nscans) =>
      def proc(m: Map[String, Int]) = {
        m.foreach { case (_, os) =>
          val oseq = Onions.toSeq(os)
          assert(oseq.size >= 1)
          if (oseq.size > 1) {
            val n = oseq.size - 1
            // TODO: don't treat all onions the same size
            addSeqScanCost += CostConstants.secToPGUnit(
              4.0 * n.toDouble *
              stats.stats(reln).row_count.toDouble / CostConstants.DiskReadBytesPerSec *
              nscans.toDouble)
          }
        }
      }
      proc(ctx.requiredOnions.getOrElse(reln, Map.empty))
      proc(ctx.precomputed.getOrElse(reln, Map.empty))
    }

    // data xfer to client cost
    val td = tupleDesc
    val bytesToXfer = td.map {
      case PosDesc(_, _, onion, vecCtx) =>
        // assume everything is 4 bytes now
        if (vecCtx) 4.0 * rr.get else 4.0
    }.sum * r.toDouble

    Estimate(
      c + aggCost + addSeqScanCost +
      subrelations.map(_._1.costEstimate(ctx, stats).cost).sum +
        CostConstants.secToPGUnit(bytesToXfer / CostConstants.NetworkXferBytesPerSec),
      r, rr.getOrElse(1), reverseStmt0, ssi)
  }

  private var _paramClassName: String = null
  private var _paramStmt: SelectStmt = null

  def emitCPPHelpers(cg: CodeGenerator) = {
    subrelations.foreach(_._1.emitCPPHelpers(cg))
      
    assert(_paramClassName eq null)
    assert(_paramStmt eq null)

    _paramClassName = "param_generator_%s".format(cg.uniqueId())
    cg.println("class %s : public sql_param_generator {".format(_paramClassName)) 
    cg.println("public:")
    cg.blockBegin("virtual param_map get_param_map(exec_context& ctx) {")

      cg.println("param_map m;")

      var i = 0
      def nextId(): Int = {
        val ret = i
        i += 1
        ret
      }

      _paramStmt = topDownTransformation(stmt) {
        case FunctionCall("encrypt", Seq(e, IntLiteral(o, _), MetaFieldIdent(fi, _)), _) =>
          assert(e.isLiteral)
          assert(BitUtils.onlyOne(o.toInt))
          assert(fi.symbol.isInstanceOf[ColumnSymbol])

          // TODO: figure out if we need join (assume we don't for now)
          val id = nextId()
          cg.println(
            "m[%d] = %s;".format(
              id, e.toCPPEncrypt(o.toInt, false, fi.symbol.asInstanceOf[ColumnSymbol])))

          val ret = (Some(QueryParamPlaceholder(id)), false)
          ret

        case FunctionCall("searchSWP", Seq(expr, pattern), _) =>
          val FunctionCall("encrypt", Seq(p, _, MetaFieldIdent(fi, _)), _) = pattern

          // assert pattern is something we can handle...
          val p0 = p.asInstanceOf[StringLiteral].v
          CollectionUtils.allIndicesOf(p0, '%').foreach { i =>
            assert(i == 0 || i == (p0.size - 1))
          }

          // split on %
          val tokens = p0.split("%").filterNot(_.isEmpty)
          assert(!tokens.isEmpty)
          if (tokens.size > 1) {
            throw new RuntimeException("cannot handle multiple tokens for now")
          }

          val id0 = nextId()
          val id1 = nextId()

          // generate code to encrypt the search token, and replace searchSWP w/ the params
          cg.blockBegin("{")
            
            cg.println(
              "Binary key(cm->getKey(cm->getmkey(), fieldname(%d, \"SWP\"), SECLEVEL::SWP));".format(
                fi.symbol.asInstanceOf[ColumnSymbol].fieldPosition))
            cg.println(
              "Token t = CryptoManager::token(key, Binary(%s));".format(
                quoteDbl(tokens.head.toLowerCase)))

            cg.println(
              "m[%d] = db_elem(t.ciph.content, t.ciph.len);".format(id0))
            cg.println(
              "m[%d] = db_elem(t.wordKey.content, t.wordKey.len);".format(id1))

          cg.blockEnd("}")

          (Some(
            FunctionCall(
              "searchSWP", Seq(QueryParamPlaceholder(id0), QueryParamPlaceholder(id1), expr))), true)

        case _ => (None, true)
      }.asInstanceOf[SelectStmt]

      cg.println("return m;")

    cg.blockEnd("}")
    cg.println("}")
  }

  def emitCPP(cg: CodeGenerator) = {
    assert(_paramClassName ne null)
    assert(_paramStmt ne null)

    cg.print("new remote_sql_op(new %s, ".format(_paramClassName))
    cg.printStr(_paramStmt.sqlFromDialect(PostgresDialect)) 
    cg.print(", %s".format( projs.map(_.toCPP).mkString("{", ", ", "}") ))
    cg.print(", {")
    subrelations.foreach(x => { 
      x._1.emitCPP(cg)
      cg.print(", ")
    })
    cg.print("})") 
  }
}

case class RemoteMaterialize(name: String, child: PlanNode) extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) =
    "* RemoteMaterialize(name = " + name + ")" + childPretty(lvl, child)

  def costEstimate(ctx: EstimateContext, stats: Statistics) = {
    val ch = child.costEstimate(ctx, stats)

    // compute cost of xfering each row back to server
    val td = tupleDesc
    val bytesToXfer = td.map {
      case PosDesc(_, _, onion, vecCtx) =>
        // assume everything is 4 bytes now
        if (vecCtx) 4.0 * ch.rowsPerRow else 4.0
    }.sum * ch.rows.toDouble
    val xferCost = CostConstants.secToPGUnit(bytesToXfer / CostConstants.NetworkXferBytesPerSec)

    ch.copy(cost = ch.cost + xferCost)
  }

  def emitCPPHelpers(cg: CodeGenerator) = child.emitCPPHelpers(cg)

  def emitCPP(cg: CodeGenerator) = throw new RuntimeException("TODO")
}

case class LocalOuterJoinFilter(
  expr: SqlExpr, origRelation: SqlRelation, posToNull: Seq[Int],
  child: PlanNode, subqueries: Seq[PlanNode]) extends PlanNode {

  {
    val td = child.tupleDesc
    def checkBounds(i: Int) = assert(i >= 0 && i < td.size)
    posToNull.foreach(checkBounds)
  }

  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) = {
    "* LocalOuterJoinFilter(filter = " + expr.sql +
      ", posToNull = " + posToNull + ")" +
      childPretty(lvl, child) +
      subqueries.map(c => childPretty(lvl, c)).mkString("")
  }

  def costEstimate(ctx: EstimateContext, stats: Statistics) = {
    val ch = child.costEstimate(ctx, stats)

    // we need to find a way to map the original relation given to the modified
    // relations given in the equivStmt.  we're currently using a heuristic
    // now, looking at join patterns for equivalences. this probably doesn't
    // capture all the cases, but since this operator is rarely needed for
    // TPC-H, we don't bother optimizing this for now

    sealed abstract trait JoinMode
    case class PrimitiveJT(name: String, alias: Option[String]) extends JoinMode
    case class MultiJT(left: JoinMode, right: JoinMode, tpe: JoinType) extends JoinMode

    def relationToJoinMode(r: SqlRelation): JoinMode =
      r match {
        case TableRelationAST(n, a, _) => PrimitiveJT(n, a)
        case JoinRelation(l, r, t, _, _) =>
          MultiJT(relationToJoinMode(l),
                  relationToJoinMode(r),
                  t)
        case _ => throw new RuntimeException("TODO: cannot handle now: " + r)
      }

    val origJoinMode = relationToJoinMode(origRelation)

    val stmt =
      resolveCheck(
        topDownTransformation(ch.equivStmt) {
          case r: SqlRelation if (relationToJoinMode(r) == origJoinMode) =>
            (Some(origRelation), false)
          case _ => (None, true)
        }.asInstanceOf[SelectStmt],
        ctx.defns)

    val (_, r, rr, _, _) = extractCostFromDBStmt(stmt, ctx.defns.dbconn.get, Some(stats))

    // TODO: estimate the cost
    Estimate(ch.cost, r, rr.getOrElse(1), stmt, ch.seqScanInfo)
  }

  def emitCPPHelpers(cg: CodeGenerator) = 
    (Seq(child) ++ subqueries).foreach(_.emitCPPHelpers(cg))

  def emitCPP(cg: CodeGenerator) = throw new RuntimeException("TODO")
}

case class LocalFilter(expr: SqlExpr, origExpr: SqlExpr,
                       child: PlanNode, subqueries: Seq[PlanNode]) extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) = {
    "* LocalFilter(filter = " + expr.sql + ")" +
      childPretty(lvl, child) +
      subqueries.map(c => childPretty(lvl, c)).mkString("")
  }

  def costEstimate(ctx: EstimateContext, stats: Statistics) = {

    // find one-time-invoke subqueries (so we can charge them once, instead of for each
    // per-row invocation)

    // returns the dependent tuple positions for each subquery
    def makeSubqueryDepMap(e: SqlExpr): Map[Int, Seq[Int]] = {
      def findTuplePositions(e: SqlExpr): Seq[Int] = {
        val m = new ArrayBuffer[Int]
        topDownTraversal(e) {
          case TuplePosition(p, _) => m += p; false
          case _                   => true
        }
        m.toSeq
      }
      val m = new HashMap[Int, Seq[Int]]
      topDownTraversal(e) {
        case SubqueryPosition(p, args, _) =>
          m += ((p -> args.flatMap(findTuplePositions))); false
        case ExistsSubqueryPosition(p, args, _) =>
          m += ((p -> args.flatMap(findTuplePositions))); false
        case _ => true
      }
      m.toMap
    }

    val m = makeSubqueryDepMap(expr)
    val td = child.tupleDesc
    val ch = child.costEstimate(ctx, stats)

    val subCosts = subqueries.map(_.costEstimate(ctx, stats)).zipWithIndex.map {
      case (costPerInvocation, idx) =>
        m.get(idx).filterNot(_.isEmpty).map { pos =>
          // check any pos in agg ctx
          if (!pos.filter(p => td(p).vectorCtx).isEmpty) {
            costPerInvocation.cost * ch.rows * ch.rowsPerRow
          } else {
            costPerInvocation.cost * ch.rows
          }
        }.getOrElse(costPerInvocation.cost)
    }.sum

    val stmt =
      resolveCheck(
        ch.equivStmt.copy(
          filter = ch.equivStmt.filter.map(x => And(x, origExpr)).orElse(Some(origExpr))),
        ctx.defns)

    val (_, r, rr, _, _) = extractCostFromDBStmt(stmt, ctx.defns.dbconn.get, Some(stats))

    // TODO: how do we cost filters?
    Estimate(ch.cost + subCosts, r, rr.getOrElse(1L), stmt, ch.seqScanInfo)
  }

  def emitCPPHelpers(cg: CodeGenerator) = 
    (Seq(child) ++ subqueries).foreach(_.emitCPPHelpers(cg))

  def emitCPP(cg: CodeGenerator) = {
    cg.print("new local_filter_op(")
    cg.print(expr.toCPP)
    cg.print(", ")
    child.emitCPP(cg)
    cg.print(", {")
    subqueries.foreach(s => {s.emitCPP(cg); cg.print(", ")})
    cg.print("})")
  }
}

case class LocalTransform(
  trfms: Seq[Either[Int, (SqlExpr, SqlExpr)]] /* (orig, translated) */, 
  child: PlanNode) extends PlanNode {

  assert(!trfms.isEmpty)

  def tupleDesc = {
    val td = child.tupleDesc
    trfms.map {
      case Left(pos) => td(pos)

      // TODO: allow for transforms to not remove vector context
      case Right((o, _)) => 
        val t = o.getType
        PosDesc(t.tpe, t.field, PlainOnion, false)
    }
  }

  def pretty0(lvl: Int) =
    "* LocalTransform(transformation = " + trfms + ")" + childPretty(lvl, child)

  def costEstimate(ctx: EstimateContext, stats: Statistics) = {
    // we assume these operations are cheap
    child.costEstimate(ctx, stats)
  }

  def emitCPPHelpers(cg: CodeGenerator) = child.emitCPPHelpers(cg)

  def emitCPP(cg: CodeGenerator) = {
    cg.print("new local_transform_op(")
    cg.print("{")
    trfms.foreach {
      case Left(i) =>
        cg.print("local_transform_op::trfm_desc(%d), ".format(i))
      case Right((orig, texpr)) =>
        val t = orig.getType
        cg.print("local_transform_op::trfm_desc(std::make_pair(%s, %s)), ".format( 
          PosDesc(t.tpe, t.field, PlainOnion, false).toCPP, 
          texpr.toCPP
        ))
    }
    cg.print("}, ")
    child.emitCPP(cg) 
    cg.print(")")
  }
}

case class LocalGroupBy(
  keys: Seq[SqlExpr], origKeys: Seq[SqlExpr],
  filter: Option[SqlExpr], origFilter: Option[SqlExpr],
  child: PlanNode, subqueries: Seq[PlanNode]) extends PlanNode {

  {
    assert(keys.size == origKeys.size)
    assert(filter.isDefined == origFilter.isDefined)
  }

  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) =
    "* LocalGroupBy(keys = " + keys.map(_.sql).mkString(", ") + ", group_filter = " + filter.map(_.sql).getOrElse("none") + ")" + childPretty(lvl, child)

  def costEstimate(ctx: EstimateContext, stats: Statistics) = {
    val ch = child.costEstimate(ctx, stats)
    assert(ch.equivStmt.groupBy.isEmpty)

    val nameSet = origKeys.flatMap {
      case FieldIdent(_, _, ColumnSymbol(tbl, col, _, _), _) =>
        Some((tbl, col))
      case _ => None
    }.toSet

    val stmt =
      resolveCheck(
        ch.equivStmt.copy(
          // need to rewrite projections
          projections = ch.equivStmt.projections.map {
            case e @ ExprProj(fi @ FieldIdent(qual, name, _, _), _, _) =>
              def wrapWithGroupConcat(e: SqlExpr) = GroupConcat(e, ",")
              qual.flatMap { q =>
                if (nameSet.contains((q, name))) Some(e) else None
              }.getOrElse(e.copy(expr = wrapWithGroupConcat(fi)))

            // TODO: not sure what to do in this case...
            case e => e
          },
          groupBy = Some(SqlGroupBy(origKeys, origFilter))),
        ctx.defns)

    val (_, r, Some(rr), _, _) = extractCostFromDBStmt(stmt, ctx.defns.dbconn.get, Some(stats))
    // TODO: estimate the cost
    Estimate(ch.cost, r, rr, stmt, ch.seqScanInfo)
  }

  def emitCPPHelpers(cg: CodeGenerator) = 
    (Seq(child) ++ subqueries).foreach(_.emitCPPHelpers(cg))

  def emitCPP(cg: CodeGenerator) = throw new RuntimeException("TODO")
}

case class LocalGroupFilter(filter: SqlExpr, origFilter: SqlExpr,
                            child: PlanNode, subqueries: Seq[PlanNode])
  extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) = {
    "* LocalGroupFilter(filter = " + filter.sql + ")" +
      childPretty(lvl, child) +
      subqueries.map(c => childPretty(lvl, c)).mkString("")
  }

  def costEstimate(ctx: EstimateContext, stats: Statistics) = {
    val ch = child.costEstimate(ctx, stats)
    // assume that the group keys are always available, for now
    assert(ch.equivStmt.groupBy.isDefined)
    val stmt =
      resolveCheck(
        ch.equivStmt.copy(
          groupBy =
            ch.equivStmt.groupBy.map(_.copy(having =
              ch.equivStmt.groupBy.get.having.map(x => And(x, origFilter)).orElse(Some(origFilter))))),
        ctx.defns)

    //println(stmt.sqlFromDialect(PostgresDialect))

    val (_, r, Some(rr), _, _) = extractCostFromDBStmt(stmt, ctx.defns.dbconn.get, Some(stats))

    // TODO: how do we cost filters?
    Estimate(ch.cost, r, rr, stmt, ch.seqScanInfo)
  }

  def emitCPPHelpers(cg: CodeGenerator) = 
    (Seq(child) ++ subqueries).foreach(_.emitCPPHelpers(cg))

  def emitCPP(cg: CodeGenerator) = throw new RuntimeException("TODO")
}

case class LocalOrderBy(sortKeys: Seq[(Int, OrderType)], child: PlanNode) extends PlanNode {
  {
    val td = child.tupleDesc
    // all sort keys must not be in vector context (b/c that would not make sense)
    sortKeys.foreach { case (idx, _) => assert(!td(idx).vectorCtx) }
  }

  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) =
    "* LocalOrderBy(keys = " + sortKeys.map(_._1.toString).toSeq + ")" + childPretty(lvl, child)

  def costEstimate(ctx: EstimateContext, stats: Statistics) = {
    // do stuff

    child.costEstimate(ctx, stats)
  }

  def emitCPPHelpers(cg: CodeGenerator) = child.emitCPPHelpers(cg)

  def emitCPP(cg: CodeGenerator) = {
    cg.print(
      "new local_order_by(%s, ".format(
        sortKeys.map { 
          case (i, o) => "std::make_pair(%d, %b)".format(i, o == DESC)
        }.mkString("{", ", ", "}")))
    child.emitCPP(cg)
    cg.print(")")
  }
}

case class LocalLimit(limit: Int, child: PlanNode) extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) =
    "* LocalLimit(limit = " + limit + ")" + childPretty(lvl, child)

  def costEstimate(ctx: EstimateContext, stats: Statistics) = {
    val ch = child.costEstimate(ctx, stats)
    // TODO: currently assuming everything must be completely materialized
    // before the limit. this isn't strictly true, but is true in the case
    // of TPC-H
    Estimate(ch.cost, math.min(limit, ch.rows), ch.rowsPerRow,
             ch.equivStmt, ch.seqScanInfo)
  }

  def emitCPPHelpers(cg: CodeGenerator) = child.emitCPPHelpers(cg)

  def emitCPP(cg: CodeGenerator) = {
    cg.print("new local_limit(%d, ".format(limit))
    child.emitCPP(cg)
    cg.print(")")
  }
}

case class LocalDecrypt(positions: Seq[Int], child: PlanNode) extends PlanNode {
  def tupleDesc = {
    val td = child.tupleDesc
    assert(positions.filter(p => td(p).onion.isPlain).isEmpty)
    assert(positions.filter(p => td(p).onion match {
            case _: HomRowDescOnion => true
            case _ => false
           }).isEmpty)
    val p0 = positions.toSet
    td.zipWithIndex.map {
      case (pd, i) if p0.contains(i) => pd.copy(onion = PlainOnion)
      case (pd, _)                   => pd
    }
  }
  def pretty0(lvl: Int) =
    "* LocalDecrypt(positions = " + positions + ")" + childPretty(lvl, child)

  def costEstimate(ctx: EstimateContext, stats: Statistics) = {
    val ch = child.costEstimate(ctx, stats)
    val td = child.tupleDesc
    def costPos(p: Int): Double = {
      td(p) match {
        case PosDesc(_, _, HomGroupOnion(tbl, grp), _) =>
          val c = CostConstants.secToPGUnit(CostConstants.DecryptCostMap(Onions.HOM))
          c * ch.rows.toDouble // for now assume that each agg group needs to do 1 decryption

        case PosDesc(_, _, RegularOnion(o), vecCtx) =>
          val c = CostConstants.secToPGUnit(CostConstants.DecryptCostMap(o))
          c * ch.rows.toDouble * (if (vecCtx) ch.rowsPerRow.toDouble else 1.0)

        case _ =>
          throw new RuntimeException("should not happen")
      }
    }
    val contrib = positions.map(costPos).sum
    ch.copy(cost = ch.cost + contrib)
  }

  def emitCPPHelpers(cg: CodeGenerator) = child.emitCPPHelpers(cg)

  def emitCPP(cg: CodeGenerator) = {
    cg.print(
      "new local_decrypt_op(%s, ".format(
        positions.map(_.toString).mkString("{", ", ", "}")))
    child.emitCPP(cg)
    cg.print(")")
  }
}

case class LocalEncrypt(
  /* (tuple pos to enc, onion to enc) */
  positions: Seq[(Int, OnionType)],
  child: PlanNode) extends PlanNode {
  def tupleDesc = {
    val td = child.tupleDesc
    assert(positions.filter { case (p, _) => !td(p).onion.isPlain || td(p).vectorCtx }.isEmpty)
    assert(positions.filter(p => p._2 match {
            case _: HomRowDescOnion => true
            case _ => false
           }).isEmpty)
    val p0 = positions.toMap
    td.zipWithIndex.map {
      case (pd, i) if p0.contains(i) => pd.copy(onion = p0(i))
      case (pd, _)                   => pd
    }
  }
  def pretty0(lvl: Int) =
    "* LocalEncrypt(positions = " + positions + ")" + childPretty(lvl, child)

  def costEstimate(ctx: EstimateContext, stats: Statistics) = {
    val ch = child.costEstimate(ctx, stats)
    val td = child.tupleDesc
    def costPos(p: (Int, OnionType)): Double = {
      p._2 match {
        case RegularOnion(o) =>
          val c = CostConstants.secToPGUnit(CostConstants.EncryptCostMap(o))
          c * ch.rows.toDouble

        case _ =>
          throw new RuntimeException("should not happen")
      }
    }
    val contrib = positions.map(costPos).sum
    ch.copy(cost = ch.cost + contrib)
  }

  def emitCPPHelpers(cg: CodeGenerator) = child.emitCPPHelpers(cg)

  def emitCPP(cg: CodeGenerator) = throw new RuntimeException("TODO")
}
