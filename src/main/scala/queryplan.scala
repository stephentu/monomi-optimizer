trait PlanNode {
  def pretty: String = pretty0(0)
  def pretty0(lvl: Int): String

  def indent(lvl: Int) = " " * (lvl * 4)
  def endl: String = "\n"
}
case class RemoteSql(stmt: SelectStmt) extends PlanNode {
  def pretty0(lvl: Int) = "* RemoteSql(sql = " + stmt.sql + ")"
}
case class LocalFilter(expr: SqlExpr, child: PlanNode) extends PlanNode {
  def pretty0(lvl: Int) = 
    "* LocalFilter(filter = " + expr.sql + ")" + endl + indent(lvl + 1) + child.pretty0(lvl + 1)
}
case class LocalGroupBy(keys: Seq[SqlExpr], child: PlanNode) extends PlanNode {
  def pretty0(lvl: Int) = 
    "* LocalGroupBy(keys = " + keys.map(_.sql).mkString(", ") + ")" + endl + indent(lvl + 1) + child.pretty0(lvl + 1)
}
case class LocalOrderBy(sortKeys: Seq[SqlExpr], child: PlanNode) extends PlanNode {
  def pretty0(lvl: Int) = 
    "* LocalOrderBy(keys = " + sortKeys.map(_.sql).mkString(", ") + ")" + endl + indent(lvl + 1) + child.pretty0(lvl + 1)
}
