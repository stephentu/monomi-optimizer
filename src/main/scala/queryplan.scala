trait PlanNode {
  // actual useful stuff

  // ( enc info, true if in vector ctx, false otherwise )
  def tupleDesc: Seq[(Option[Int], Boolean)]

  // printing stuff
  def pretty: String = pretty0(0)

  protected def pretty0(lvl: Int): String
  protected def childPretty(lvl: Int, child: PlanNode): String = 
    endl + indent(lvl + 1) + child.pretty0(lvl + 1)

  def indent(lvl: Int) = " " * (lvl * 4)
  def endl: String = "\n"
}
case class RemoteSql(stmt: SelectStmt, projs: Seq[(Option[Int], Boolean)]) extends PlanNode {
  def tupleDesc = projs
  def pretty0(lvl: Int) = "* RemoteSql(sql = " + stmt.sql + ", projs = " + projs + ")"
}
case class LocalFilter(expr: SqlExpr, child: PlanNode, subqueries: Seq[PlanNode]) extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) = { 
    "* LocalFilter(filter = " + expr.sql + ")" + 
      childPretty(lvl, child) + 
      subqueries.map(c => childPretty(lvl, c)).mkString("")
  }
}
case class LocalTransform(trfms: Seq[Either[Int, SqlExpr]], child: PlanNode) extends PlanNode {
  def tupleDesc = {
    val td = child.tupleDesc
    trfms.map {
      case Left(pos) => td(pos)
      // TODO: allow for transforms to not remove vector context
      case Right(_) => (None, false)
    }
  }
  def pretty0(lvl: Int) = 
    "* LocalTransform(transformation = " + trfms + ")" + childPretty(lvl, child)
}
case class LocalGroupBy(keys: Seq[SqlExpr], filter: Option[SqlExpr], child: PlanNode) extends PlanNode {
  def tupleDesc = throw new RuntimeException("unimpl")
  def pretty0(lvl: Int) = 
    "* LocalGroupBy(keys = " + keys.map(_.sql).mkString(", ") + ", group_filter = " + filter.map(_.sql).getOrElse("none") + ")" + childPretty(lvl, child)
}
case class LocalGroupFilter(filter: SqlExpr, child: PlanNode, subqueries: Seq[PlanNode]) extends PlanNode {
  def tupleDesc = child.tupleDesc 
  def pretty0(lvl: Int) = {
    "* LocalGroupFilter(filter = " + filter.sql + ")" + 
      childPretty(lvl, child) + 
      subqueries.map(c => childPretty(lvl, c)).mkString("")
  }
}
case class LocalOrderBy(sortKeys: Seq[(Int, OrderType)], child: PlanNode) extends PlanNode {
  def tupleDesc = child.tupleDesc 
  def pretty0(lvl: Int) = 
    "* LocalOrderBy(keys = " + sortKeys.map(_._1.toString).toSeq + ")" + childPretty(lvl, child)
}
case class LocalLimit(limit: Int, child: PlanNode) extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) = 
    "* LocalLimit(limit = " + limit + ")" + childPretty(lvl, child)
}

case class LocalDecrypt(positions: Seq[Int], child: PlanNode) extends PlanNode {
  def tupleDesc = {
    val td = child.tupleDesc
    assert(positions.filter(p => !td(p)._1.isDefined).isEmpty)
    val p0 = positions.toSet
    td.zipWithIndex.map { 
      case ((Some(_), c), i) if p0.contains(i) => (None, c)
      case (e, _) => e
    }
  }
  def pretty0(lvl: Int) = 
    "* LocalDecrypt(positions = " + positions + ")" + childPretty(lvl, child)
}

case class LocalEncrypt(
  /* (tuple pos to enc, onion to enc) */
  positions: Seq[(Int, Int)], 
  child: PlanNode) extends PlanNode {
  def tupleDesc = {
    val td = child.tupleDesc
    assert(positions.filter { case (p, _) => td(p)._1.isDefined }.isEmpty)
    val p0 = positions.toMap
    td.zipWithIndex.map { 
      case ((None, c), i) if p0.contains(i) => (Some(p0(i)), c)
      case (e, _) => e
    }
  }
  def pretty0(lvl: Int) = 
    "* LocalEncrypt(positions = " + positions + ")" + childPretty(lvl, child)
}
