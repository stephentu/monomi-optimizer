package edu.mit.cryptdb.tpch

import edu.mit.cryptdb._
import java.util.concurrent._
import collection.mutable.HashMap

object RobustTester extends PlanTransformers {
  // This is a nasty hack
  // XXX: workaround the fact that context objects still may exist in AST
  // nodes of plan nodes- eventually we should clean this up, so that
  // PlanNodes have no context objects in their ASTs
  def sanitizePlanNode(p: PlanNode): PlanNode = {
    @inline def fix[N <: Node](s: N): N = s.withoutContextT[N]

    def trfm(p: PlanNode): PlanNode =
      topDownTransformation(p) {
        case RemoteSql(stmt, p, ss, ns) =>
          (Some(RemoteSql(fix(stmt), p,
                          ss.map { case (r, s) => (r, fix(s)) },
                          ns.map { case (k, (p, s)) => (k, (p, fix(s))) })), true)

        case LocalOuterJoinFilter(e, or, p, ch, sq) =>
          (Some(LocalOuterJoinFilter(fix(e), fix(or), p, ch, sq)), true)

        case LocalFilter(e, oe, c, sq, f) =>
          (Some(LocalFilter(fix(e), fix(oe), c, sq, f)), true)

        case LocalTransform(t, os, c) =>
          (Some(LocalTransform(t.map(x => x match {
                case Left(i) => Left(i)
                case Right((e, o, e0)) => Right((fix(e), o, fix(e0)))
              }), fix(os), c)), true)

        case LocalGroupBy(k, ok, f, of, c, sq) =>
          (Some(LocalGroupBy(k.map(fix), ok.map(fix), f.map(fix), of.map(fix), c, sq)), true)

        case LocalGroupFilter(f, of, c, sq) =>
          (Some(LocalGroupFilter(fix(f), fix(of), c, sq)), true)

        case e =>
          (Some(e), true)
      }

    trfm(p)
  }

  def workloadDiff(lhs: Seq[PlanNode], rhs: Seq[PlanNode]):
    Seq[(Int, PlanNode, PlanNode)] = {
    assert(lhs.size == rhs.size)
    val lhs0 = lhs.map(sanitizePlanNode)
    val rhs0 = rhs.map(sanitizePlanNode)
    lhs0.zip(rhs0).zipWithIndex.flatMap {
      case ((lhs, rhs), idx) =>
        if (lhs != rhs)
          Some((idx, lhs, rhs))
        else
          None
    }
  }

  // used by the test runners to avoid running redundant plans. the
  // first seq returned by findCommonPlans will be a seq of 0s
  def findCommonPlans(allPlans: Seq[ Seq[PlanNode] ]):
    Seq[ Seq[Int] ] = {
    assert(!allPlans.isEmpty)
    allPlans.tail.foreach { x => assert(x.size == allPlans.head.size) }
    val allPlans0 = allPlans.map(_.map(sanitizePlanNode))
    val leftmost = allPlans0.head.map { p =>
      val x = new HashMap[PlanNode, Int]
      x += ((p, 0))
      x
    }
    allPlans0.zipWithIndex.map { case (plans, myIdx) =>
      plans.zip(leftmost).map { case (p, m) =>
        m.get(p) match {
          case Some(idx) => idx
          case None =>
            m += ((p, myIdx))
            myIdx
        }
      }
    }
  }
}

class RobustTester extends Timer {

  import RobustTester._

  case class Info(
    worstCase: (Seq[Int], Seq[(PlanNode, Double)], Double),
    bestCase: (Seq[Int], Seq[(PlanNode, Double)], Double),
    // allCosts must is sorted
    allCosts: Seq[Double]) {

    assert(allCosts.sorted == allCosts)

    def worstCasePlans: Seq[PlanNode] = worstCase._2.map(_._1)
    def bestCasePlans : Seq[PlanNode] = bestCase._2.map(_._1)

    // array of points (cost, cdf_value), which means
    // cdf_value fraction of all executions executed with
    // cost time or less
    def cdf: Seq[(Double, Double)] = {
      val n = allCosts.size
      allCosts.zipWithIndex.map { case (cost, idx) =>
        (cost, (idx + 1).toDouble / n)
      }
    }
  }

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

  private val _thdLocalGenerator = new ThreadLocal[Generator] {
    protected override def initialValue = new Generator with DefaultOptimizerConfiguration
  }

  private val _thdLocalRuntimeOptimizer = new ThreadLocal[RuntimeOptimizer] {
    protected override def initialValue = new RuntimeOptimizer with DefaultOptimizerConfiguration
  }

  private def simulate(
    schema: Definitions, stats: Statistics,
    queries: Seq[SelectStmt], n: Int): Info = {

    assert(!queries.isEmpty)
    assert(n >= 1 && n <= queries.size)

    println("simulate(%d)".format(n))

    val i = new atomic.AtomicInteger(0)

    val futures = (0 until queries.size).combinations(n).map { case idxs =>
      def runner(): Seq[(PlanNode, Double)] = {
        val train = CollectionUtils.slice(queries, idxs)

        val (time0, plans) =
          timedRunMillis(_thdLocalGenerator.get().generateCandidatePlans(train))

        val (time1, costs) =
          timedRunMillis(plans.map(_.map { case (p, ctx) => (p, ctx, p.costEstimate(ctx, stats)) }))

        val simple = costs.map(_.sortBy(_._3.cost))

        // recover the physical design from the solution
        val design =
          simple.map(_.head._2.makeRequiredOnionSet).foldLeft(new OnionSet) {
            case (acc, os) => acc.merge(os)
          }.complete(schema)

        val (time2, ret) =
          timedRunMillis(queries.map(x => _thdLocalRuntimeOptimizer.get().optimize(design, stats, x)))

        //println("time0 = %f ms".format(time0))
        //println("time1 = %f ms".format(time1))
        //println("time2 = %f ms".format(time2))

        ret.map { case (p, _, est) => (p, est.cost) }
      }

      val callable = new Callable[(Seq[Int], Seq[(PlanNode, Double)], Double)] {
        def call() = {
          val res = runner()
          val cost = res.map(_._2).sum
          val x = i.incrementAndGet()
          if ((x % 100) == 0) {
            println("  Finished excuting %d simulations (cacheHitRate=%f)...".format(
              x, _thdLocalGenerator.get().cacheHitRate * 100.0))
          }
          (idxs, res, cost)
        }
      }

      thdPool.submit(callable)
    }.toIndexedSeq // force evaluation

    val res = futures.map(_.get())
    val sorted = res.sortBy(_._3)

    val ret = Info(sorted.last, sorted.head, sorted.map(_._3))

    println("simulate(%d): worstIdxs: %s, worstCost: %f, bestIdxs: %s, bestCost: %f".format(
      n,
      ret.worstCase._1.mkString("[", ", ", "]"),
      ret.worstCase._3,
      ret.bestCase._1.mkString("[", ", ", "]"),
      ret.bestCase._3))

    ret
  }
}
