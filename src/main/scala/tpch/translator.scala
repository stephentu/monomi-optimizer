package edu.mit.cryptdb.tpch

import edu.mit.cryptdb._
import edu.mit.cryptdb.user._

// must be completely stateless
class TPCHTranslator extends Translator {

  def translateTableName(plainTableName: String) =
    plainTableName match {
      case "customer" | "lineitem" | "part" | "partsupp" |
           "region" | "nation" | "orders" | "supplier" =>
        plainTableName + "_enc_cryptdb_opt_with_det"
      case _ =>
        plainTableName
    }

  @inline
  private def colName(name: String, onion: Int): String =
    name + "_" + Onions.str(onion)

  def translateColumnName(
    plainTableName: String, plainColumnName: String, encOnion: Int) = {
    colName(plainTableName, encOnion)
  }

  def translatePrecomputedExprName(
    exprId: String, plainTableName: String, expr: SqlExpr, encOnion: Int): String = {

    // enumerate all the interesting ones

    // customer
    if (plainTableName == "customer") {
      expr match {
        case Substring(FieldIdent(None, "c_phone", _, _), 1, Some(2), _) =>
          return colName("c_phone_prefix", encOnion)
        case _ =>
      }

    // lineitem
    } else if (plainTableName == "lineitem") {
      expr match {
        case Extract(FieldIdent(None, "l_shipdate", _, _), YEAR, _) =>
          return colName("l_shipdate_year", encOnion)
        case Mult(FieldIdent(None, "l_extendedprice", _, _), Minus(IntLiteral(1, _), FieldIdent(None, "l_discount", _, _), _), _) =>
          return colName("l_disc_price", encOnion)
        case _ =>
      }

    // orders
    } else if (plainTableName == "orders") {
      expr match {
        case Extract(FieldIdent(None, "o_orderdate", _, _), YEAR, _) =>
          return colName("o_orderdate_year", encOnion)
        case _ =>
      }
    }

    // default case
    exprId
  }

  private val FSPrefix = "/space/stephentu/data"

  def filenameForHomAggGroup(
    aggId: Int, plainDbName: String, plainTableName: String, aggs: Seq[SqlExpr]): String = {

    val p = FSPrefix + "/" + plainDbName + "/" + plainTableName + "_enc"

    // enumerate all the interesting ones

    println("filenameForHomAggGroup: table %s".format(plainTableName))
    println("  " + aggs.map(_.sql).mkString("[", ", ", "]"))

    // customer
    if (plainTableName == "customer") {
      aggs match {
        case Seq(FieldIdent(None, "c_acctbal", _, _)) =>
          return p + "/row_pack/acctbal"
        case _ =>
      }
    }

    // lineitem
    else if (plainTableName == "lineitem") {
      aggs match {
        case Seq(
            FieldIdent(None, "l_quantity", _, _),
            FieldIdent(None, "l_extendedprice", _, _),
            FieldIdent(None, "l_discount", _, _),
            // (l_extendedprice * (1 - l_discount))
            Mult(FieldIdent(None, "l_extendedprice", _, _), Minus(IntLiteral(1, _), FieldIdent(None, "l_discount", _, _), _), _),
            // ((l_extendedprice * (1 - l_discount)) * (1 + l_tax))
            Mult(Mult(FieldIdent(None, "l_extendedprice", _, _), Minus(IntLiteral(1, _), FieldIdent(None, "l_discount", _, _), _), _), Plus(IntLiteral(1, _), FieldIdent(None, "l_tax", _, _), _), _)
          ) =>

          // XXX: ordering might be off
          return p + "/row_col_pack/data"
        case _ =>
      }
    }

    // partsupp
    else if (plainTableName == "partsupp") {
      aggs match {
        case Seq(Mult(FieldIdent(None, "ps_supplycost", _, _), FieldIdent(None, "ps_availqty", _, _), _)) =>
          return p + "/row_pack/volume"
        case _ =>
      }
    }

    // default case
    p + "/agg_" + aggId
  }

}
