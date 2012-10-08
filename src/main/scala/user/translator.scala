package edu.mit.cryptdb.user

import edu.mit.cryptdb.SqlExpr

// implementations should be completely stateless
trait Translator {

  def translateTableName(
    plainTableName: String): String

  def translateColumnName(
    plainTableName: String, plainColumnName: String, encOnion: Int): String

  def translatePrecomputedExprName(
    exprId: String, plainTableName: String, expr: SqlExpr, encOnion: Int): String

  def filenameForHomAggGroup(
    aggId: Int, plainDbName: String, plainTableName: String, aggs: Seq[SqlExpr]): String

}
