trait PlanNode {
  def pretty: String = pretty0(0)
  def pretty0(lvl: Int): String
  def childPretty(lvl: Int, child: PlanNode): String = 
    endl + indent(lvl + 1) + child.pretty0(lvl + 1)

  def indent(lvl: Int) = " " * (lvl * 4)
  def endl: String = "\n"
}
case class RemoteSql(stmt: SelectStmt) extends PlanNode {
  def pretty0(lvl: Int) = "* RemoteSql(sql = " + stmt.sql + ")"
}
case class LocalFilter(expr: SqlExpr, child: PlanNode) extends PlanNode {
  def pretty0(lvl: Int) = 
    "* LocalFilter(filter = " + expr.sql + ")" + childPretty(lvl, child)
}
case class LocalTransform(expr: SqlExpr, child: PlanNode) extends PlanNode {
  def pretty0(lvl: Int) = 
    "* LocalTransform(transformation = " + expr.sql + ")" + childPretty(lvl, child)
}
case class LocalGroupBy(keys: Seq[SqlExpr], filter: Option[SqlExpr], child: PlanNode) extends PlanNode {
  def pretty0(lvl: Int) = 
    "* LocalGroupBy(keys = " + keys.map(_.sql).mkString(", ") + ", group_filter = " + filter.map(_.sql).getOrElse("none") + ")" + childPretty(lvl, child)
}
case class LocalGroupFilter(filter: SqlExpr, child: PlanNode) extends PlanNode {
  def pretty0(lvl: Int) =
    "* LocalGroupFilter(filter = " + filter.sql + ")" + childPretty(lvl, child)
}
case class LocalOrderBy(sortKeys: Seq[(SqlExpr, OrderType)], child: PlanNode) extends PlanNode {
  def pretty0(lvl: Int) = 
    "* LocalOrderBy(keys = " + sortKeys.map(_._1.sql).mkString(", ") + ")" + childPretty(lvl, child)
}
case class LocalLimit(limit: Int, child: PlanNode) extends PlanNode {
  def pretty0(lvl: Int) = 
    "* LocalLimit(limit = " + limit + ")" + childPretty(lvl, child)
}
