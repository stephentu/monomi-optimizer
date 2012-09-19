package edu.mit.cryptdb.tpch

import edu.mit.cryptdb._

class RobustTester {

  case class Info(
    worstCase: Seq[(PlanNode, Double)],
    bestCase: Seq[(PlanNode, Double)])

  private object resolver extends Resolver
  private object generator extends Generator with DefaultOptimizerConfiguration
  private object runtimeOptimizer extends RuntimeOptimizer with DefaultOptimizerConfiguration

  private val parser = new SQLParser

  // for each i in [1, 2, ..., queries.size()]:
  //   find the i queries which, when trained on, produce the worst/best
  //      sum(Run(Q1), Run(Q2), ..., Run(QN))
  def simulate(schema: Schema, queries: Seq[String]): Map[Int, Info] = {
    val sa = schema.loadSchema()
    val st = schema.loadStats()

    val un = queries.map(parser.parse(_))
    val re = un.map(x => resolver.resolve(x.get, sa))

    (1 to queries.size).map(i => (i, simulate(sa, st, re, i))).toMap
  }

  private def simulate(
    schema: Definitions, stats: Statistics,
    queries: Seq[SelectStmt], n: Int): Info = {

    assert(n >= 1 && n <= queries.size)

    println("simulate(%d)".format(n))

    var worst: Option[Seq[(PlanNode, Double)]] = None
    var best: Option[Seq[(PlanNode, Double)]] = None

    // all idxs for all (queries.size choose n) perms
    var i = 0
    (0 until queries.size).combinations(n).foreach { case idxs =>
      val train = CollectionUtils.slice(queries, idxs)
      val plans = generator.generateCandidatePlans(train)
      val costs = plans.map(_.map { case (p, ctx) => (p, ctx, p.costEstimate(ctx, stats)) })
      val simple = costs.map(_.sortBy(_._3.cost))

      // recover the physical design from the solution
      val design =
        simple.map(_.head._2.makeRequiredOnionSet).foldLeft(new OnionSet) {
          case (acc, os) => acc.merge(os)
        }.complete(schema)

      val finalPlans = queries.map(x => runtimeOptimizer.optimize(design, stats, x))
      val thisCost = finalPlans.map(_._2).sum

      worst match {
        case Some(xs) =>
          if (xs.map(_._2).sum < thisCost) {
            worst = Some(finalPlans)
          }
        case None =>
          worst = Some(finalPlans)
      }

      best match {
        case Some(xs) =>
          if (xs.map(_._2).sum > thisCost) {
            best = Some(finalPlans)
          }
        case None =>
          best = Some(finalPlans)
      }

      i += 1
      if ((i % 1000) == 0) {
        println("  *** completed %d simulations".format(i))
      }
    }

    assert(worst.isDefined && best.isDefined)
    Info(worst.get, best.get)
  }
}
