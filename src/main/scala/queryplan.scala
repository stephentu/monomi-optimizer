trait PlanNode {
  // actual useful stuff
  def tupleDesc: Seq[Option[Int]]

  // printing stuff
  def pretty: String = pretty0(0)

  protected def pretty0(lvl: Int): String
  protected def childPretty(lvl: Int, child: PlanNode): String = 
    endl + indent(lvl + 1) + child.pretty0(lvl + 1)

  def indent(lvl: Int) = " " * (lvl * 4)
  def endl: String = "\n"
}
case class RemoteSql(stmt: SelectStmt, projs: Seq[Option[Int]]) extends PlanNode {
  def tupleDesc = projs
  def pretty0(lvl: Int) = "* RemoteSql(sql = " + stmt.sql + ")"
}
case class LocalFilter(expr: SqlExpr, child: PlanNode, subqueries: Seq[PlanNode]) extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) = 
    "* LocalFilter(filter = " + expr.sql + ")" 
      + childPretty(lvl, child) 
      + subqueries.map(c => childPretty(lvl, c)).mkString("")
}
case class LocalTransform(transformations: Seq[Either[Int, SqlExpr]], child: PlanNode) extends PlanNode {
  def tupleDesc = {
    

  }
  def pretty0(lvl: Int) = 
    "* LocalTransform(transformation = " + transformations + ")" + childPretty(lvl, child)
}
case class LocalGroupBy(keys: Seq[SqlExpr], filter: Option[SqlExpr], child: PlanNode) extends PlanNode {
  def pretty0(lvl: Int) = 
    "* LocalGroupBy(keys = " + keys.map(_.sql).mkString(", ") + ", group_filter = " + filter.map(_.sql).getOrElse("none") + ")" + childPretty(lvl, child)
}
case class LocalGroupFilter(filter: SqlExpr, child: PlanNode, subqueries: Seq[PlanNode]) extends PlanNode {
  def pretty0(lvl: Int) =
    "* LocalGroupFilter(filter = " + filter.sql + ")" 
      + childPretty(lvl, child)
      + subqueries.map(c => childPretty(lvl, c)).mkString("")
}
case class LocalOrderBy(sortKeys: Seq[(Int, OrderType)], child: PlanNode) extends PlanNode {
  def pretty0(lvl: Int) = 
    "* LocalOrderBy(keys = " + sortKeys.map(_._1.toString).mkString(", ") + ")" + childPretty(lvl, child)
}
case class LocalLimit(limit: Int, child: PlanNode) extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) = 
    "* LocalLimit(limit = " + limit + ")" + childPretty(lvl, child)
}

case class LocalDecrypt(positions: Seq[Int], child: PlanNode) extends PlanNode {
  def tupleDesc = {
    val td = child.tupleDesc
    assert(positions.filter(p => !td(p).isDefined).isEmpty)
    val p0 = positions.toSet
    td.zipWithIndex.map { 
      case (Some(_), i) if p0.contains(i) => None
      case e => e
    }
  }
  def pretty0(lvl: Int) = 
    "* LocalDecrypt(positions = " + positions + ")" + childPretty(lvl, child)
}

case class LocalEncrypt(positions: Seq[Int], child: PlanNode) extends PlanNode {
  def pretty0(lvl: Int) = 
    "* LocalEncrypt(positions = " + positions + ")" + childPretty(lvl, child)
}
