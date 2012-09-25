package edu.mit.cryptdb.tpch

import edu.mit.cryptdb._
import java.util.concurrent._

class RobustTester extends Timer {

  case class Info(
    worstCase: (Seq[Int], Seq[(PlanNode, Double)], Double),
    bestCase: (Seq[Int], Seq[(PlanNode, Double)], Double))

  private object resolver extends Resolver
  private val parser = new SQLParser

  // for each i in [1, 2, ..., queries.size()]:
  //   find the i queries which, when trained on, produce the worst/best
  //      sum(Run(Q1), Run(Q2), ..., Run(QN))
  def simulate(schema: Schema, queries: Seq[String],
               start: Option[Int] = None, stop: Option[Int] = None):
    Map[Int, Info] = {

    start.foreach { x =>
      assert(x >= 1 && x <= queries.size)
      stop.foreach(y => assert(x <= y))
    }
    stop.foreach(x => assert(x >= 1 && x <= queries.size))

    val sa = schema.loadSchema()
    val st = schema.loadStats()

    val un = queries.map(parser.parse(_))
    val re = un.map(x => resolver.resolve(x.get, sa))

    (start.getOrElse(1) to stop.getOrElse(queries.size))
      .map(i => (i, simulate(sa, st, re, i))).toMap
  }

  private lazy val thdPool =
    Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors * 2)

  private def simulate(
    schema: Definitions, stats: Statistics,
    queries: Seq[SelectStmt], n: Int): Info = {

    assert(!queries.isEmpty)
    assert(n >= 1 && n <= queries.size)

    println("simulate(%d)".format(n))

    val i = new atomic.AtomicInteger(0)

    val futures = (0 until queries.size).combinations(n).map { case idxs =>
      def runner(): Seq[(PlanNode, Double)] = {
        object generator extends Generator with DefaultOptimizerConfiguration
        object runtimeOptimizer extends RuntimeOptimizer with DefaultOptimizerConfiguration

        val train = CollectionUtils.slice(queries, idxs)
        val plans = generator.generateCandidatePlans(train)
        val costs = plans.map(_.map { case (p, ctx) => (p, ctx, p.costEstimate(ctx, stats)) })
        val simple = costs.map(_.sortBy(_._3.cost))

        // recover the physical design from the solution
        val design =
          simple.map(_.head._2.makeRequiredOnionSet).foldLeft(new OnionSet) {
            case (acc, os) => acc.merge(os)
          }.complete(schema)

        queries.map(x => runtimeOptimizer.optimize(design, stats, x))
      }

      val callable = new Callable[(Seq[Int], Seq[(PlanNode, Double)], Double)] {
        def call() = {
          val res = runner()
          val cost = res.map(_._2).sum
          val x = i.incrementAndGet()
          if ((x % 100) == 0) {
            println("  Finished excuting %d simulations...".format(x))
          }
          (idxs, res, cost)
        }
      }

      thdPool.submit(callable)
    }.toIndexedSeq // force evaluation

    val res = futures.map(_.get())
    val sorted = res.sortBy(_._3)

    val ret = Info(sorted.last, sorted.head)

    println("simulate(%d): worstIdxs: %s, worstCost: %f, bestIdxs: %s, bestCost: %f".format(
      n,
      ret.worstCase._1.mkString("[", ", ", "]"),
      ret.worstCase._3,
      ret.bestCase._1.mkString("[", ", ", "]"),
      ret.bestCase._3))

    ret
  }
}
