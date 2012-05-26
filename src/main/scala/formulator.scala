package edu.mit.cryptdb

import scala.collection.mutable.{ HashMap, Seq => MutSeq }

// these are the onions which a plan cost has to
// be concerned about (b/c it does seq scans over these relations),
// except it does NOT use these onions (so it is the non-constant
// part of the cost function)
case class PlanExtraCosts(
  regExtraOnions: Map[String, Map[String, Int]],
  precomputedExtraOnions: Map[String, Map[String, Int]])

package object math_defns {
  type ImmVector[A] = scala.collection.immutable.Vector[A]
  type ImmMatrix[A] = ImmVector[ImmVector[A]]

  type MutVector[A] = scala.collection.mutable.Seq[A]
  type MutMatrix[A] = MutVector[MutVector[A]]

  def toImmVector[A](v: MutVector[A]): ImmVector[A] = {
    scala.collection.immutable.Vector(v:_*)
  }

  def toImmMatrix[A](m: MutMatrix[A]): ImmMatrix[A] = {
    scala.collection.immutable.Vector(m.map(toImmVector(_)):_*)
  }

  def validate[A](m: ImmMatrix[A]): Boolean = {
    // validate matrix is well-formed
    m.headOption.map { h =>
      val s = h.size
      m.filter(_.size != s).isEmpty
    }.getOrElse(false)
  }

  def is_square[A](m: ImmMatrix[A]): Boolean = {
    assert(validate(m))
    if (m.isEmpty) true
    else m.size == m.head.size
  }

  def dimensions[A](m: ImmMatrix[A]): (Int, Int) = {
    assert(validate(m))
    if (m.isEmpty) ((0, 0))
    else ((m.size, m.head.size))
  }

  def matrixAsMCode[A](m: ImmMatrix[A]): String = {
    m.map(v => vectorAsMCode(v, false)).mkString("[", ";", "]")
  }

  def vectorAsMCode[A](v: ImmVector[A], c: Boolean): String = {
    v.map(_.toString).mkString("[", (if (c) ";" else ","), "]")
  }
}

import math_defns._

// an instance of a binary-integer-quadratic-program
//
// this program is the following optimization problem:
//
// min 0.5x'Qx + cx
// subject to:
//    A x <= b
//    Aeq x = beq
//    x is binary
case class BIQPInstance(
  Q: ImmMatrix[Double],
  c: ImmVector[Double],
  ieq_constraint: Option[ (ImmMatrix[Double], ImmVector[Double]) ],
  eq_constraint: Option[ (ImmMatrix[Double], ImmVector[Double]) ]) {

  assert(is_square(Q))
  assert(Q.size == c.size)

  val n = Q.size

  private def validateConstraint[A](matrix: ImmMatrix[A], vector: ImmVector[A]) = {
    val (m, n0) = dimensions(matrix)
    assert(n == n0)
    assert(m == vector.size)
  }

  ieq_constraint.foreach { case (a, b) => validateConstraint(a, b) }
  eq_constraint.foreach  { case (a, b) => validateConstraint(a, b) }

  def toMCode: String = {
    val buf = new StringBuilder

    def writeMatrix[A](name: String, m: ImmMatrix[A]) = {
      buf.append(name)
      buf.append(" = ")
      buf.append(matrixAsMCode(m))
      buf.append(";\n")
    }

    def writeVector[A](name: String, v: ImmVector[A], c: Boolean) = {
      buf.append(name)
      buf.append(" = ")
      buf.append(vectorAsMCode(v, c))
      buf.append(";\n")
    }

    writeMatrix("Q", Q)
    writeVector("c", c, true)

    ieq_constraint match {
      case Some((a, b)) =>
        writeMatrix("A", a)
        writeVector("b", b, true)

      case None =>
        buf.append("A = [];\n")
        buf.append("b = [];\n")
    }

    eq_constraint match {
      case Some((aeq, beq)) =>
        writeMatrix("Aeq", aeq)
        writeVector("beq", beq, true)

      case None =>
        buf.append("Aeq = [];\n")
        buf.append("beq = [];\n")
    }

    buf.append("Options = struct('method', 'breadth', 'maxQPiter', 100000);\n")
    buf.append("[xmin, fmin, flag, Extendedflag] = ")
    buf.append("miqp(Q, c, A, b, Aeq, beq, [1:%d], [], [], [], Options);\n".format(n))

    buf.toString
  }

  def solve(): Option[ImmVector[Boolean]] = {
    throw new RuntimeException("UNIMPL")
  }
}

trait Formulator {

  def optimize(
    defns: Definitions,
    stats: Statistics,
    queries: Seq[ Seq[(PlanNode, EstimateContext, Estimate)] ]): Seq[PlanNode] = {

    // TODO: use the quadratic program formulation

    // for now, just ignore the cross query effects and optimize purely locally

    queries.map { candidates =>
      // find the min cost candidate
      candidates.sortBy(_._3.cost).head._1
    }
  }

  private def toImm(hm: HashMap[String, HashMap[String, Int]]):
    Map[String, Map[String, Int]] = {
    hm.map { case (k, v) => (k, v.toMap) }.toMap
  }

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
    defns: Definitions,
    stats: Statistics,
    queries: Seq[ Seq[(PlanNode, EstimateContext, Estimate)] ]): BIQPInstance = {

    // assumes queries is not empty
    val globalOpts = queries.head.head._2.globalOpts

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

    val globalOnions = GlobalOnions(toImm(reg), toImm(pre))

    val extraCosts =
      queries.map(_.map { case (_, ec, e) => extraCostsFromEstimate(globalOnions, ec, e) })

    var _ctr = 0
    def nextPos(): Int = {
      val x = _ctr
      _ctr += 1
      x
    }
    def numVariables: Int = _ctr

    // have the query plan selection flags come first
    queries.foreach(_.foreach(_ => nextPos()))

    // maps (reln, col, onion) => position in the X vector
    val globalXAssignReg = new HashMap[String, HashMap[(String, Int), Int]]

    // maps (reln, group) => position in the X vector
    val globalXAssignHom = new HashMap[String, HashMap[Int, Int]]

    // regular columns
    defns.defns.foreach { case (tbl, cols) =>
      cols.foreach {
        case TableColumn(name, _) =>
          reg.get(tbl).flatMap(_.get(name)).foreach { o =>
            val os = Onions.toSeq(o)
            if (os.size > 1) {
              os.foreach { oo =>
                assert(Onions.isSingleRowEncOnion(oo))
                globalXAssignReg.getOrElseUpdate(tbl, HashMap.empty).put((name, oo), nextPos())
              }
            }
          }
      }
    }

    // pre-computed columns
    pre.foreach { case (reln, cols) =>
      cols.foreach {
        case (name, os) =>
          Onions.toSeq(os).foreach { o =>
            assert(Onions.isSingleRowEncOnion(o))
            globalXAssignReg.getOrElseUpdate(reln, HashMap.empty).put((name, o), nextPos())
          }
      }
    }

    // hom agg groups
    globalOpts.homGroups.foreach { case (reln, groups) =>
      // sanity check
      assert( groups.toSet.size == groups.size )
      (0 until groups.size).foreach { g =>
        globalXAssignHom.getOrElseUpdate(reln, HashMap.empty).put(g, nextPos())
      }
    }

    // build cost function in normal form:
    //   0.5 x' Q x + c x
    // note that Q_{ij} = 2H_{ij}, where H_{ij} is the coefficent you want
    // in front of x_{i}x_{j} in the final equation
    //
    // specifically in this formulation, we'll let x_{i} be the decision variable
    // governing whether or not to pick the query plan, and let x_{j} be the
    // decision variable governing whether or not a column optimization is enabled.
    // H_{ij} = (number of times table(column j) is scanned in plan i) * (cost to scan column j)
    //
    // for query plan i, we pick only the x_{j} such that the j-th onion is not
    // actually required for the plan to function, but exists in a table which is
    // scanned in the plan
    //
    // the fixed cost for a plan (which includes only the required onion
    // (assumes the plan is running independently)) is captured in the c vector
    // c_{i} is the fixed cost for the i-th query plan

    def mkNewMatrix(m: Int, n: Int): MutMatrix[Double] = MutSeq.fill(m, n)(0.0)
    def mkNewSqMatrix(n: Int): MutMatrix[Double] = mkNewMatrix(n, n)
    def mkNewVector(n: Int): MutVector[Double] = MutSeq.fill(n)(0.0)

    val Q_arg = mkNewSqMatrix(numVariables)
    val c_arg = mkNewVector(numVariables)

    val A_arg = mkNewMatrix(1, numVariables)
    val b_arg = mkNewVector(1)

    val Aeq_arg = mkNewMatrix(queries.size, numVariables)
    val beq_arg = mkNewVector(queries.size)

    queries.zipWithIndex.foldLeft(0) { case ( acc, ( plans, qidx ) ) =>
      plans.zipWithIndex.foreach { case ( ( nodes, ectx, est ), idx ) =>
        val pidx = acc + idx

        c_arg(pidx) = est.cost // fixed cost

        // for each table we do sequential scan on
        est.seqScanInfo.foreach { case (tbl, nscans) =>

          // s := total_onions(tbl) - used_onions(tbl)
          // add each element in s to Q (scaled by nscans)

          val gTable: HashMap[(String, Int), Int] = globalXAssignReg.getOrElse(tbl, HashMap.empty)
          val sTable = stats.stats(tbl)

          // total_onions comes from reg+pre
          def proc(m: HashMap[String, HashMap[String, Int]],
                   b: Map[String, Map[String, Int]]) = {
            m.get(tbl).foreach { m =>
              b.get(tbl).foreach { b =>
                def procOnions(k: String, os: Seq[Int]) = {
                  os.foreach { o =>
                    gTable.get((k, o)).foreach { globalId =>
                      // TODO: scale accordingly
                      val recordSize = 4.0
                      val cost =
                        CostConstants.secToPGUnit(
                          nscans *
                          (recordSize *
                           sTable.row_count.toDouble / CostConstants.DiskReadBytesPerSec))
                      Q_arg(pidx)(globalId) = 2.0 * cost

                      // constraints!
                      // each query plan has all the necessary onions + precomp onions it needs
                      // (we add hom group constraints separately, below)
                      //
                      // we formulate as follows: suppose plan i requires onions q = [x1, x2, ...]
                      // each plan i contributes the following inequality constraint:
                      //   \sum_{q} x_{q} - |q|x_{i} \geq 0
                      // this constraint says that if we pick x_{i}, then we must have all [x1, x2, ...] \in q
                      // to be enabled

                      A_arg(0)(globalId) += 1.0 // onion
                      A_arg(0)(pidx) -= 1.0 // query
                    }
                  }
                }
                m.foreach {
                  case (k, v) if !b.contains(k) => procOnions(k, Onions.toSeq(v))
                  case (k, v) => procOnions(k, (Onions.toSeq(v).toSet -- Onions.toSeq(b(k)).toSet).toSeq)
                }
              }
            }
          }

          proc(reg, ectx.requiredOnions)
          proc(pre, ectx.precomputed)

          val gHomTable: HashMap[Int, Int] = globalXAssignHom.getOrElse(tbl, HashMap.empty)
          ectx.homGroups.get(tbl).foreach(_.foreach { g =>
            val globalId = gHomTable(g)
            A_arg(0)(globalId) += 1.0
            A_arg(0)(pidx) -= 1.0
          })
        }

      }

      // each query picks exactly 1 plan
      // (equality constraints)
      //
      // this formulation is simple:
      // suppose query 1 has plans k = [p1, p2, ...]
      // each query contributes:
      //   \sum_{k} x_{k} = 1
      // this says for each query we must pick exactly one execution option. really, the equality
      // is not necessary (is sufficient to say \geq 1), but b/c we are computing min cost,
      // the min cost option must always be to pick exactly one execution plan

      (0 until plans.size).foreach { idx =>
        val pidx = acc + idx
        Aeq_arg(qidx)(pidx) = 1.0
      }
      beq_arg(qidx) = 1.0

      // TODO: storage constraint

      (acc + plans.size)
    }

    BIQPInstance(toImmMatrix(Q_arg), toImmVector(c_arg),
                 Some((toImmMatrix(A_arg), toImmVector(b_arg))),
                 Some((toImmMatrix(Aeq_arg), toImmVector(beq_arg))))
  }

}
