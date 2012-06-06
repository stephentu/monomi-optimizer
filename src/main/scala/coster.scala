package edu.mit.cryptdb

// just a few helpers
trait Coster extends Timer {
  private def costPlan0(plan: Seq[(PlanNode, EstimateContext)], stats: Statistics, name: String):
    Seq[(PlanNode, EstimateContext, Estimate)] = {
    println("estimate cost for query %s, %d plan nodes...".format(name, plan.size))
    val (time, res) = 
    timedRunMillis(
      plan.zipWithIndex.map { case ((p, ctx), idx) => 
        val (time, res) = timedRunMillis((p, ctx, p.costEstimate(ctx, stats)))
        //println("  cost estimate for plan %d took %f ms".format(idx, time))
        res
      })
    println("  cost estimate for all plans took %f ms".format(time))
    res
  }

  def costPlan(plan: Seq[(PlanNode, EstimateContext)], stats: Statistics):
    Seq[(PlanNode, EstimateContext, Estimate)] = { costPlan0(plan, stats, "0") }

  def costPlans(plans: Seq[ Seq[(PlanNode, EstimateContext)] ], stats: Statistics): 
    Seq[ Seq[(PlanNode, EstimateContext, Estimate)] ] = {
    plans.zipWithIndex.map { case (p, idx) => costPlan0(p, stats, idx.toString) }
  }
}
