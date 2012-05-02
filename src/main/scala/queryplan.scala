//case class EstimateContext(
//  // translate encrypted identifiers back into plain text identifiers
//  // (but keep the structure of the query the same)
//  reverseTranslateMap: IdentityHashMap[Node, Node]) {
//
//  def reverseTranslate[N <: Node](n: N): Option[N] =
//    reverseTranslateMap.get(n).asInstanceOf[Option[N]]
//
//}

import scala.util.parsing.json._

case class EstimateContext(
  defns: Definitions,
  precomputed: Map[String, SqlExpr],
  needsRowIDs: Set[String])

case class Estimate(
  cost: Double,
  rows: Long,
  rowsPerRow: Long /* estimate cardinality within each aggregate group */)

trait PlanNode {
  // actual useful stuff

  // ( enc info, true if in vector ctx, false otherwise )
  def tupleDesc: Seq[(Option[Int], Boolean)]

  def costEstimate(ctx: EstimateContext): Estimate

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

    def basename(s: String): String =
      if (s.contains("$")) s.split("\\$").dropRight(1).mkString("$")
      else s

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
            if (ctx.defns.tableExists(qual0)) {
              // rowids are rewritten to 0
              println("ctx.needsRowIDs: " + ctx.needsRowIDs)
              println("name0: " + name0)
              if (ctx.needsRowIDs.contains(qual0) && name0 == "rowid") {
                Some((Some(IntLiteral(0)), false))
              } else {
                // try to rewrite just table
                Some((Some(FieldIdent(Some(qual0), name)), false))
              }
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

      case FunctionCall("encrypt", Seq(e, _), _) =>
        (Some(e), false)

      case _ => (None, true)
    }.asInstanceOf[SelectStmt]

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

    def extractInfoFromQueryPlan(node: Map[String, Any]):
      (Double, Long, Option[Long]) = {

      val firstChild =
        for (L(children) <- node.get("Plans");
             M(child) = children.head) yield extractInfoFromQueryPlan(child)

      val Some((totalCost, planRows)) = for (
        D(totalCost) <- node.get("Total Cost");
        D(planRows)  <- node.get("Plan Rows")
      ) yield ((totalCost, planRows.toLong))

      node("Node Type") match {
        case "Aggregate" =>
          // must have firstChild
          assert(firstChild.isDefined)

          firstChild.get match {
            case (_, rows, None) =>
              (totalCost,
               planRows,
               Some(math.ceil(rows.toDouble / planRows.toDouble).toLong))

            // TODO: agg of aggs??
          }

        case _ =>
          // simple case, just read from this node only
          firstChild.map {
            case (_, _, x) => (totalCost, planRows, x)
          }.getOrElse((totalCost, planRows, None))
      }
    }

    val sql = reverseStmt.sqlFromDialect(PostgresDialect)

    println(sql)

    import Conversions._

    val r = ctx.defns.dbconn.get.getConn.createStatement.executeQuery("EXPLAIN (FORMAT JSON) " + sql)
    val res = r.map { rs =>
      val planJson = JSON.parseFull(rs.getString(1))
      //println(planJson)
      (for (L(l) <- planJson;
            M(m) = l.head;
            M(p) <- m.get("Plan")) yield extractInfoFromQueryPlan(p)).getOrElse(
        throw new RuntimeException("unexpected return from postgres: " + planJson)
      )
    }

    Estimate(res.head._1, res.head._2, res.head._3.getOrElse(1))
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
