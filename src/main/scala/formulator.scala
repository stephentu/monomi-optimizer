package edu.mit.cryptdb

import scala.collection.mutable.HashMap

// these are the onions which a plan cost has to
// be concerned about (b/c it does seq scans over these relations),
// except it does NOT use these onions (so it is the non-constant
// part of the cost function)
case class PlanExtraCosts(
  regExtraOnions: Map[String, Map[String, Int]],
  precomputedExtraOnions: Map[String, Map[String, Int]])

trait Formulator {

  private case class GlobalOnions(
    regular: Map[String, Map[String, Int]],
    precomputed: Map[String, Map[String, Int]])

  private def extraCostsFromEstimate(
    global: GlobalOnions, ctx: EstimateContext, est: Estimate):
    PlanExtraCosts = {

    // we make a simplification and say the extra costs are only from
    // doing sequential scans

    val reg = new HashMap[String, HashMap[String, Int]]
    val pre = new HashMap[String, HashMap[String, Int]]

    def mergeDiff(
      dest: HashMap[String, HashMap[String, Int]],
      src: Map[String, Map[String, Int]],
      global: Map[String, Map[String, Int]]) = {

      src.foreach { case (k, v) =>
        val gm = global(k)
        v.foreach {
          case (colname, onion) =>
            assert((gm(colname) & onion) == onion) // assert the global contains this instance
            if (gm(colname) != onion) {
              // need to place the difference in the dest map

              val dm = dest.getOrElseUpdate(k, new HashMap[String, Int])
              assert(!dm.contains(colname))
              dm.put(colname, (gm(colname) & (~onion)))
            }
        }
      }
    }

    val seqScanTbls = est.seqScanInfo.map(_._1).toSet

    mergeDiff(
      reg,
      ctx.requiredOnions.filter(t => seqScanTbls.contains(t._1)),
      global.regular)

    mergeDiff(
      pre,
      ctx.precomputed.filter(t => seqScanTbls.contains(t._1)),
      global.precomputed)

    PlanExtraCosts(
      reg.map { case (k, v) => (k, v.toMap) }.toMap,
      pre.map { case (k, v) => (k, v.toMap) }.toMap)
  }

  // give as input:
  // seq ( seq( (rewritten plan, est ctx, cost estimate for plan) ) )
  def formulateIntegerProgram(
    queries: Seq[ Seq[(PlanNode, EstimateContext, Estimate)] ]): Unit = {

    val reg = new HashMap[String, HashMap[String, Int]]
    val pre = new HashMap[String, HashMap[String, Int]]

    queries.foreach(_.foreach { case (_, ctx, _) =>
      def mergeInto(dest: HashMap[String, HashMap[String, Int]],
                    src: Map[String, Map[String, Int]]) = {
        src.foreach { case (k, v) =>
          val m = dest.getOrElseUpdate(k, new HashMap[String, Int])
          v.foreach { case (k, v) => m.put(k, m.getOrElse(k, 0) | v) }
        }
      }

      mergeInto(reg, ctx.requiredOnions)
      mergeInto(pre, ctx.precomputed)
    })

  }

}
