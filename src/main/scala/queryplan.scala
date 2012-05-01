//case class EstimateContext(
//  // translate encrypted identifiers back into plain text identifiers
//  // (but keep the structure of the query the same)
//  reverseTranslateMap: IdentityHashMap[Node, Node]) {
//
//  def reverseTranslate[N <: Node](n: N): Option[N] =
//    reverseTranslateMap.get(n).asInstanceOf[Option[N]]
//
//}

case class EstimateContext(
  defns: Definitions,
  precomputed: Map[String, SqlExpr])

trait PlanNode {
  // actual useful stuff

  // ( enc info, true if in vector ctx, false otherwise )
  def tupleDesc: Seq[(Option[Int], Boolean)]

  def costEstimate(ctx: EstimateContext): Long

  // printing stuff
  def pretty: String = pretty0(0)

  protected def pretty0(lvl: Int): String
  protected def childPretty(lvl: Int, child: PlanNode): String =
    endl + indent(lvl + 1) + child.pretty0(lvl + 1)

  protected def indent(lvl: Int) = " " * (lvl * 4)
  protected def endl: String = "\n"
}

case class RemoteSql(stmt: SelectStmt,
                     projs: Seq[(Option[Int], Boolean)],
                     subrelations: Seq[PlanNode] = Seq.empty)
  extends PlanNode with Transformers {

  assert(stmt.projections.size == projs.size)
  def tupleDesc = projs
  def pretty0(lvl: Int) = {
    "* RemoteSql(sql = " + stmt.sql + ", projs = " + projs + ")" +
    subrelations.map(c => childPretty(lvl, c)).mkString("")
  }

  def costEstimate(ctx: EstimateContext) = {
    // TODO: this is very hacky, and definitely prone to error
    // but it's a lot of mindless work to propagate the precise
    // node replacement information, so we just use some text
    // substitution for now, and wait to implement a real solution

    def basename(s: String): String = s.split("\\$").dropRight(1).mkString("$")

    def rewriteWithQual[N <: Node](n: N, q: String): N =
      topDownTransformation(n) {
        case f: FieldIdent => (Some(f.copy(qualifier = Some(q))), false)
        case _ => (None, true)
      }.asInstanceOf[N]

    val reverseStmt = topDownTransformation(stmt) {
      case FieldIdent(Some(qual), name, _, _) =>
        // check precomputed first
        val qual0 = basename(qual)
        val name0 = basename(name)
        ctx.precomputed.get(name0)
          .map(x => (Some(rewriteWithQual(x, qual0)), false))
          .orElse {
            // try to rewrite table + column
            ctx.defns.lookup(qual0, name0).map { _ =>
              (Some(FieldIdent(Some(qual0), name0)), false) }
          }
          .orElse {
            // try to rewrite just table
            if (ctx.defns.tableExists(qual0)) {
              Some((Some(FieldIdent(Some(qual0), name)), false))
            } else {
              None
            }
          }.getOrElse((None, true))

      case TableRelationAST(name, alias, _) =>
        val name0 = basename(name)
        if (ctx.defns.tableExists(name0)) {
          (Some(TableRelationAST(name0, alias)), false)
        } else {
          (None, false)
        }
      case _ => (None, true)
    }.asInstanceOf[SelectStmt]

    println(reverseStmt.sql)

    0L
  }
}

case class RemoteMaterialize(name: String, child: PlanNode) extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) =
    "* RemoteMaterialize(name = " + name + ")" + childPretty(lvl, child)

  def costEstimate(ctx: EstimateContext) = {
    // do stuff

    child.costEstimate(ctx)
  }
}

case class LocalFilter(expr: SqlExpr, child: PlanNode, subqueries: Seq[PlanNode]) extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) = {
    "* LocalFilter(filter = " + expr.sql + ")" +
      childPretty(lvl, child) +
      subqueries.map(c => childPretty(lvl, c)).mkString("")
  }

  def costEstimate(ctx: EstimateContext) = {
    // do stuff

    child.costEstimate(ctx)
  }
}

case class LocalTransform(trfms: Seq[Either[Int, SqlExpr]], child: PlanNode) extends PlanNode {
  assert(!trfms.isEmpty)
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

  def costEstimate(ctx: EstimateContext) = {
    // do stuff

    child.costEstimate(ctx)
  }
}

case class LocalGroupBy(keys: Seq[SqlExpr], filter: Option[SqlExpr], child: PlanNode) extends PlanNode {
  def tupleDesc = throw new RuntimeException("unimpl")
  def pretty0(lvl: Int) =
    "* LocalGroupBy(keys = " + keys.map(_.sql).mkString(", ") + ", group_filter = " + filter.map(_.sql).getOrElse("none") + ")" + childPretty(lvl, child)

  def costEstimate(ctx: EstimateContext) = {
    // do stuff

    child.costEstimate(ctx)
  }
}

case class LocalGroupFilter(filter: SqlExpr, child: PlanNode, subqueries: Seq[PlanNode]) extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) = {
    "* LocalGroupFilter(filter = " + filter.sql + ")" +
      childPretty(lvl, child) +
      subqueries.map(c => childPretty(lvl, c)).mkString("")
  }

  def costEstimate(ctx: EstimateContext) = {
    // do stuff

    child.costEstimate(ctx)
  }
}

case class LocalOrderBy(sortKeys: Seq[(Int, OrderType)], child: PlanNode) extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) =
    "* LocalOrderBy(keys = " + sortKeys.map(_._1.toString).toSeq + ")" + childPretty(lvl, child)

  def costEstimate(ctx: EstimateContext) = {
    // do stuff

    child.costEstimate(ctx)
  }
}

case class LocalLimit(limit: Int, child: PlanNode) extends PlanNode {
  def tupleDesc = child.tupleDesc
  def pretty0(lvl: Int) =
    "* LocalLimit(limit = " + limit + ")" + childPretty(lvl, child)

  def costEstimate(ctx: EstimateContext) = {
    // do stuff

    child.costEstimate(ctx)
  }
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

  def costEstimate(ctx: EstimateContext) = {
    // do stuff

    child.costEstimate(ctx)
  }
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

  def costEstimate(ctx: EstimateContext) = {
    // do stuff

    child.costEstimate(ctx)
  }
}
