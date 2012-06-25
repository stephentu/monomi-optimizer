package edu.mit.cryptdb

import java.io.{ File, PrintWriter }
import scala.collection.mutable.{ ArrayBuffer, HashSet, HashMap, Seq => MutSeq }

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

// an instance of a binary-integer-linear-program
//
// this program is the following optimization problem:
//
// min cx
// subject to:
//    A x <= b
//    Aeq x = beq
//    x is binary
case class BILPInstance(
  c: ImmVector[Double],
  ieq_constraint: Option[ (ImmMatrix[Double], ImmVector[Double]) ],
  eq_constraint: Option[ (ImmMatrix[Double], ImmVector[Double]) ]) {

  private val n = c.size

  private def validateConstraint[A](matrix: ImmMatrix[A], vector: ImmVector[A]) = {
    val (m, n0) = dimensions(matrix)
    assert(n == n0)
    assert(m == vector.size)
  }

  ieq_constraint.foreach { case (a, b) => validateConstraint(a, b) }
  eq_constraint.foreach  { case (a, b) => validateConstraint(a, b) }

  def toCPLEXInput: String = {
    val buf = new StringBuilder

    buf.append("Minimize\n")

    def vecToString(v: ImmVector[Double]): String = {
      v.zipWithIndex.flatMap { case (d, idx) =>
        if (d != 0.0) {
          val s = if (d > 0.0) "+" else "-"
          Seq("%s %f x%d".format(s, math.abs(d), idx))
        }
        else Seq.empty
      }.mkString(" ")
    }

    buf.append("R0: " + vecToString(c) + "\n")

    buf.append("Subject To\n")

    ieq_constraint.foreach { case (mat, vec) =>
      val (n, _) = dimensions(mat)
      (0 until n).foreach { r =>
        buf.append("R%d: ".format(r+1) + vecToString(mat(r)) + " <= " + vec(r) + "\n")
      }
    }

    eq_constraint.foreach { case (mat, vec) =>
      val (n, _) = dimensions(mat)
      val offset = ieq_constraint.map(x => dimensions(x._1)._1).getOrElse(0)
      (0 until n).foreach { r =>
        buf.append("R%d: ".format(r+1+offset) + vecToString(mat(r)) + " = " + vec(r) + "\n")
      }
    }

    buf.append("Bounds\n")
    (0 until n).foreach { r =>
      buf.append("0 <= x%d <= 1\n".format(r))
    }

    buf.append("Generals\n")
    (0 until n).foreach { r =>
      buf.append("x%d\n".format(r))
    }

    buf.append("end\n")

    buf.toString
  }

  def toLPSolveInput: String = {
    val buf = new StringBuilder

    def vecToString(v: ImmVector[Double], omitZero: Boolean): String = {
      v.zipWithIndex.flatMap { case (d, idx) =>
        if (!omitZero || d != 0.0) Seq("%f x%d".format(d, idx))
        else Seq.empty
      }.mkString(" ")
    }

    buf.append("min: " + vecToString(c, true) + ";\n")

    ieq_constraint.foreach { case (mat, vec) =>
      val (n, _) = dimensions(mat)
      (0 until n).foreach { r =>
        buf.append(vecToString(mat(r), true) + " <= " + vec(r) + ";\n")
      }
    }

    eq_constraint.foreach { case (mat, vec) =>
      val (n, _) = dimensions(mat)
      (0 until n).foreach { r =>
        buf.append(vecToString(mat(r), true) + " = " + vec(r) + ";\n")
      }
    }

    buf.append("bin " + (0 until n).map(x => "x%d".format(x)).mkString(", ") + ";\n")
    buf.toString
  }

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

    buf.append("[x,fval,exitflag,output] = ")
    buf.append("bintprog(c, A, b, Aeq, beq);\n".format(n))

    buf.toString
  }

  private def solveUsingLPSolve(): Option[(Double, ImmVector[Boolean])] = {
    // using lp_solve in java is a PITA.
    // just shell out for now.
    // TODO: this is so hacky

    val tmpFile = File.createTempFile("cdbopt", "lp")
    val code = toLPSolveInput

    val writer = new PrintWriter(tmpFile)
    writer.print(code)
    writer.flush

    val lp_solve_prog =
      Option(System.getenv("LP_SOLVE")).filterNot(_.isEmpty).getOrElse("lp_solve")

    val output = ProcUtils.execCommandWithResults("%s %s".format(lp_solve_prog, tmpFile.getPath))

    val objtext = output.filter(_.startsWith("Value of objective function:"))
    assert(objtext.size == 1)
    val obj = objtext.head.split(":")(1).toDouble

    val vtext = output.filter(_.startsWith("x"))
    assert(vtext.size == n)

    // lp_solve seems to give the variables back in sorted order,
    // so this is ok...
    Some( (obj, Vector( vtext.map(_.split("\\s+")(1) == "1") : _* )) )
  }

  private def solveUsingGLPK(): Option[(Double, ImmVector[Boolean])] = {
    val tmpFile = File.createTempFile("cdbopt", "lp")
    val code = toCPLEXInput

    val writer = new PrintWriter(tmpFile)
    writer.print(code)
    writer.flush

    val glpsol_prog =
      Option(System.getenv("GLPSOL_PROGRAM")).filterNot(_.isEmpty).getOrElse("glpsol")

    val output = ProcUtils.execCommandWithResults("%s --lp %s --output /dev/stdout".format(glpsol_prog, tmpFile.getPath))

    val objtext = output.filter(_.startsWith("Objective:"))
    assert(objtext.size == 1)
    val obj = objtext.head.split("\\s+")(3).toDouble

    val VarRegex = "x\\d+".r

    val values = output.flatMap { l =>
      // hacky for now...
      val toks = l.trim.split("\\s+")

      if (toks.size >= 2) {
        toks(1) match {
          case VarRegex() =>
            assert(toks(2) == "*")
            Seq(toks(3) == "1")
          case _ => Seq.empty
        }
      } else Seq.empty
    }

    assert(values.size == n)
    Some( (obj, Vector( values : _* )) )
  }

  protected val UseGLPK = true

  def solve(): Option[(Double, ImmVector[Boolean])] = {
    if (UseGLPK) {
      solveUsingGLPK()
    } else {
      solveUsingLPSolve()
    }
  }
}

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

  private val n = Q.size

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

  def solve(): Option[(Double, ImmVector[Boolean])] = {
    throw new RuntimeException("UNIMPL")
  }

  // ignore the higher order terms of this program
  def reduceToBILP: BILPInstance =
    BILPInstance(c, ieq_constraint, eq_constraint)
}

/** estimates in bytes */
trait SpaceEstimator {

  def plaintextSpaceEstimate(
    defns: Definitions,
    stats: Statistics): Long = {
    defns.defns.foldLeft(0L) { case (sizeSoFar, (tbl, cols)) =>
      val nRowsTable = stats.stats(tbl).row_count
      cols.foldLeft(sizeSoFar) {
        case (acc, TableColumn(_, tpe, _)) =>
          val regColSize = tpe.size
          acc + regColSize * nRowsTable
      }
    }
  }

  def encryptedSpaceEstimate(
    defns: Definitions,
    stats: Statistics,
    plans: Seq[EstimateContext]): Long = {

    // assumes queries is not empty
    val globalOpts = plans.head.globalOpts

    val usedReg = new HashMap[String, HashMap[String, Int]]
    val usedPre = new HashMap[String, HashMap[String, Int]]
    val usedHom = new HashMap[String, HashSet[Int]]

    plans.foreach { ectx =>
      mergeInto(usedReg, ectx.requiredOnions)
      mergeInto(usedPre, ectx.precomputed)
      ectx.homGroups.foreach { case (reln, gids) =>
        usedHom.getOrElseUpdate(reln, HashSet.empty) ++= gids
      }
    }

    var sum = 0L

    defns.defns.foreach { case (tbl, cols) =>
      val nRowsTable = stats.stats(tbl).row_count
      cols.foreach {
        case TableColumn(name, tpe, _) =>
          usedReg.get(tbl).flatMap(_.get(name)) match {
            case Some(onions) =>
              Onions.toSeq(onions).foreach { onion =>
                sum += nRowsTable * encColSize(tpe.size, onion)
              }
            case None =>
              // exists in DET
              sum += nRowsTable * tpe.size
          }
      }
    }

    usedPre.foreach { case (tbl, m) =>
      val nRowsTable = stats.stats(tbl).row_count
      m.values.foreach { onions =>
        Onions.toSeq(onions).foreach { o =>
          sum += nRowsTable * encColSize(4, o)
        }
      }
    }

    usedHom.foreach { case (tbl, gids) =>
      val nRowsTable = stats.stats(tbl).row_count
      gids.foreach { gid =>
        sum += homAggSize(globalOpts, tbl, gid, nRowsTable)
      }
    }

    sum
  }

  protected def encColSize(regColSize: Int, o: Int): Long = {
    o match {
      case Onions.DET => regColSize
      case Onions.OPE => regColSize * 2
      case Onions.SWP => regColSize * 3 // estimate only
      case _ => throw new RuntimeException("Unhandled onion: " + o);
    }
  }

  protected def homAggSize(
    globalOpts: GlobalOpts, reln: String, gid: Int, nRows: Long): Long = {
    val nExprs = globalOpts.homGroups(reln)(gid).size

    val nRowsPerHomAgg = math.ceil(12.0 / nExprs.toDouble).toLong
    val nAggs = math.ceil(nRows.toDouble / nRowsPerHomAgg.toDouble).toLong

    val nBitsPerAgg = 83 // TODO: this is hardcoded in our system in various places

    val bytesPerAgg =
      math.max(
        nExprs.toDouble * nRowsPerHomAgg.toDouble * nBitsPerAgg.toDouble * 2.0 / 8.0,
        256.0).toLong

    println(
      "homGroup(%s,%d): nExprs = %d, nRowsPerHomAgg = %d, bytesPerAgg = %d, nAggs=%d, totalSize=%s".format(
      reln, gid, nExprs, nRowsPerHomAgg, bytesPerAgg, nAggs, (nAggs * bytesPerAgg).toString))

    nAggs * bytesPerAgg
  }

  protected def mergeInto(dest: HashMap[String, HashMap[String, Int]],
                src: Map[String, Map[String, Int]]) = {
    src.foreach { case (k, v) =>
      val m = dest.getOrElseUpdate(k, new HashMap[String, Int])
      v.foreach { case (k, v) => m.put(k, m.getOrElse(k, 0) | v) }
    }
  }
}

trait Formulator extends SpaceEstimator {

  val SpaceFactor = 10.0 // default

  val SpaceConstraintScaleFactor = 100000000.0 // scales bytes by this amount
  val ObjectiveFunctionScaleFactor = 1000000.0 // scales cost by this amount

  def optimize(
    defns: Definitions,
    stats: Statistics,
    queries0: Seq[ Seq[(PlanNode, EstimateContext, Estimate)] ]): Seq[PlanNode] = {

    val queries = queries0.map(_.sortBy(_._3.cost))

    if (queries.filter(_.size > 1).isEmpty) {
      // simple case
      return queries.map(_.head._1)
    }

    // use linear program for now
    val progInstance = formulateIntegerProgram(defns, stats, queries)

    val bilp = progInstance.prog.reduceToBILP
    val (obj, soln) = bilp.solve().getOrElse(throw new RuntimeException("No solution brah!"))

    val optSoln = new ArrayBuffer[(PlanNode, EstimateContext)]
    queries.foldLeft(0) { case (base, plans) =>
      val interest = soln.slice(base, base + plans.size)
      val cands = interest.zipWithIndex.filter(_._1)
      assert(cands.size == 1) // assert exactly one solution
      val solnIdx = cands.head._2
      val (p, ectx, _) = plans(solnIdx)
      optSoln += ((p, ectx))

      // sanity check that the required x variables have been set
      (ectx.requiredOnions ++ ectx.precomputed).foreach { case (reln, cols) =>
        val m = progInstance.globalXAssignReg.get(reln).getOrElse(Map.empty)
        cols.foreach { case (name, onions) =>
          Onions.toSeq(onions).foreach { o =>
            m.get((name, o)).foreach { pos =>
              if (!soln(pos)) {
                println(
                  "ERROR: solution does not contain required x-variable id=(%d), opt=(%s, %s, %s)".format(
                    pos, reln, name, Onions.str(o)))
              }
            }
          }
        }
      }

      ectx.homGroups.foreach { case (reln, gids) =>
        val m = progInstance.globalXAssignHom.get(reln).getOrElse(Map.empty)
        gids.foreach { gid =>
          m.get(gid).foreach { pos =>
            if (!soln(pos)) {
              println(
                "ERROR: solution does not contain required x-variable id=(%d), homGroup=(%s, %s)".format(
                  pos, reln, gid))
            }
          }
        }
      }

      (base + plans.size)
    }

    val simpleSoln = queries.map(_.head._1) // b/c queries is already sorted by simpleSoln

    assert(optSoln.size == simpleSoln.size) // sanity

    // useful debugging reporting
    println("LP objective function value: " + obj)

    optSoln.map(_._1).zip(simpleSoln).zipWithIndex.foreach {
      case ((a, b), idx) if a != b =>
        println("Query %d differs in choice between opt and simple solution".format(idx))
      case _ =>
    }

    val origSpace = plaintextSpaceEstimate(defns, stats)
    val encSpace = encryptedSpaceEstimate(defns, stats, optSoln.map(_._2))

    println("origSpace=(%f MB), encSpace=(%f MB), overhead=(%f)".format(
      origSpace.toDouble / (1 << 20).toDouble,
      encSpace.toDouble / (1 << 20).toDouble,
      encSpace.toDouble / origSpace.toDouble))

    optSoln.map(_._1).toSeq
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

  case class ProgramInstance(
    prog: BIQPInstance,
    globalXAssignReg: Map[String, Map[(String, Int), Int]],
    globalXAssignHom: Map[String, Map[Int, Int]])

  // give as input:
  // seq ( seq( (rewritten plan, est ctx, cost estimate for plan) ) )
  def formulateIntegerProgram(
    defns: Definitions,
    stats: Statistics,
    queries: Seq[ Seq[(PlanNode, EstimateContext, Estimate)] ]): ProgramInstance = {

    // assumes queries is not empty
    val globalOpts = queries.head.head._2.globalOpts

    val reg = new HashMap[String, HashMap[String, Int]]
    val pre = new HashMap[String, HashMap[String, Int]]

    queries.zipWithIndex.foreach { case (qplans, qidx) =>
      qplans.zipWithIndex.foreach { case ((_, ctx, _), pidx) =>
        //println("q%d_p%d requiredOnions:".format(qidx, pidx) + ctx.requiredOnions)

        mergeInto(reg, ctx.requiredOnions)
        mergeInto(pre, ctx.precomputed)
      }
    }

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
    // THIS MUST COME FIRST, b/c the callers assume this (and the order of
    // queries MUST be preserved)
    queries.foreach(_.foreach(_ => nextPos()))

    // maps (reln, col, onion) => position in the X vector
    val globalXAssignReg = new HashMap[String, HashMap[(String, Int), Int]]

    // maps (reln, group) => position in the X vector
    val globalXAssignHom = new HashMap[String, HashMap[Int, Int]]

    // regular columns
    defns.defns.foreach { case (tbl, cols) =>
      cols.foreach {
        case TableColumn(name, _, _) =>
          reg.get(tbl).flatMap(_.get(name)).foreach { o =>
            val os = Onions.toSeq(o)
            // if there's only one choice, we're gonna pick it regardless,
            // so it's pointless to add it as a variable
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
      println("relation " + reln + " precomputed expressions:")
      cols.foreach {
        case (name, os) =>
          println("  %s: %s".format(name, globalOpts.precomputed(reln)(name)))
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
      println("relation " + reln + " hom groups:")
      (0 until groups.size).foreach { g =>
        println("  %d: %s".format(g, groups(g).map(_.sql).mkString("{", ", ", "}")))
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

    // 0: storage constraint
    // 1...: number of total plans
    val nTotalPlans = queries.foldLeft(0) { case (acc, plans) => acc + plans.size }
    val A_arg = mkNewMatrix(1 + nTotalPlans, numVariables)
    val b_arg = mkNewVector(1 + nTotalPlans)

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
        }

        // constraints!
        // each query plan has all the necessary onions + precomp onions it needs
        // (we add hom group constraints separately, below)
        //
        // we formulate as follows: suppose plan i requires onions q = [x1, x2, ...]
        // each plan i contributes the following inequality constraint:
        //   \sum_{q} x_{q} - |q|x_{i} \geq 0
        // this constraint says that if we pick x_{i}, then we must have all [x1, x2, ...] \in q
        // to be enabled

        // TODO: we really want an index which maps a plan to all the x's used

        (ectx.requiredOnions ++ ectx.precomputed).foreach { case (reln, m) =>
          val gTable: HashMap[(String, Int), Int] =
            globalXAssignReg.getOrElse(reln, HashMap.empty)
          m.foreach { case (col, o) =>
            Onions.toSeq(o).foreach { o0 =>
              gTable.get((col, o0)).foreach { gid =>
                A_arg(pidx + 1)(gid) -= 1.0 // onion
                A_arg(pidx + 1)(pidx) += 1.0 // query
              }
            }
          }
        }

        ectx.homGroups.foreach { case (reln, gids) =>
          val gHomTable: HashMap[Int, Int] =
            globalXAssignHom.getOrElse(reln, HashMap.empty)
          gids.foreach { g =>
            val globalId = gHomTable(g)
            A_arg(pidx + 1)(globalId) -= 1.0
            A_arg(pidx + 1)(pidx) += 1.0
          }
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

      (acc + plans.size)
    }

    // storage constraint:
    // the storage constraint is a linear function of x:
    // the constraint can be expressed as Ax <= b, where b is the constant
    // which says don't exceed this space

    // regular
    defns.defns.foreach { case (tbl, cols) =>
      val nRowsTable = stats.stats(tbl).row_count
      val gTable: HashMap[(String, Int), Int] = globalXAssignReg.getOrElse(tbl, HashMap.empty)
      cols.foreach {
        case TableColumn(name, tpe, _) =>
          val regColSize = tpe.size
          b_arg(0) += SpaceFactor * nRowsTable * regColSize
          reg.get(tbl).flatMap(_.get(name)) match {
            case Some(o) =>
              val os = Onions.toSeq(o)
              if (os.size > 1) {
                os.foreach { o0 =>
                  A_arg(0)(gTable((name, o0))) = nRowsTable * encColSize(regColSize, o0)
                }
              } else {
                // if there's only one choice, then we don't create an x variable
                // for the choice. however, if the onion is different size, we need
                // to factor that into the space calculation
                b_arg(0) -= nRowsTable * encColSize(regColSize, os.head)
              }
            case None =>
              // no queries ever touch this column, so it will exist in DET form
              b_arg(0) -= nRowsTable * regColSize
          }
      }
    }

    // precomputed
    pre.foreach { case (tbl, cols) =>
      val nRowsTable = stats.stats(tbl).row_count
      val gTable: HashMap[(String, Int), Int] = globalXAssignReg.getOrElse(tbl, HashMap.empty)
      cols.foreach { case (name, onions) =>
        // lookup precomp type
        Onions.toSeq(onions).foreach { o =>
          // TODO: actually propagate type information for
          // precomputed values- right now we just assume they are all
          // 4 bytes
          A_arg(0)(gTable((name, o))) = nRowsTable * encColSize(4, o)
        }
      }
    }

    // hom agg
    globalXAssignHom.foreach { case (tbl, grps) =>
      val nRowsTable = stats.stats(tbl).row_count
      println("table: " + tbl)
      grps.toSeq.sortBy(_._1).foreach { case (gid, idx) =>
        A_arg(0)(idx) = homAggSize(globalOpts, tbl, gid, nRowsTable).toDouble
      }
    }

    println("x vector assignments")

    queries.zipWithIndex.foldLeft(0) { case (acc, (plans, idx)) =>
      // inclusive
      println("q%d: x%d-x%d".format(idx, acc, acc + plans.size - 1))
      acc + plans.size
    }

    globalXAssignReg.flatMap { case (reln, m) =>
      m.map { case ((col, onion), idx) =>
        (idx, reln, col, onion)
      }
    }.toSeq.sortBy(_._1).map { case (idx, reln, col, onion) =>
      println("%d: %s:%s:%d".format(idx, reln, col, onion))
    }

    globalXAssignHom.flatMap { case (reln, m) =>
      m.map { case (gid, idx) =>
        (idx, reln, gid)
      }
    }.toSeq.sortBy(_._1).map { case (idx, reln, gid) =>
      println("%d: %s:%d".format(idx, reln, gid))
    }

    // Do not allow any objective value to exceed 5 orders of magnitude of the
    // smallest value, by capping values larger
    val smallest = c_arg.filterNot(_ == 0.0).min
    val maxLimit = smallest * 100000.0
    var nCapped = 0
    val c_arg_capped = c_arg.map { x => if (x > maxLimit) { nCapped +=1;  maxLimit } else x }

    if (nCapped > 0) {
      println("WARNING: had to cap %d values to %f".format(nCapped, maxLimit))
    }

    // apply scaling
    val c_arg_scaled = c_arg_capped.map(_ / ObjectiveFunctionScaleFactor)
    A_arg(0) = A_arg(0).map(_ / SpaceConstraintScaleFactor)
    b_arg(0) = b_arg(0) / SpaceConstraintScaleFactor

    ProgramInstance(
      BIQPInstance(toImmMatrix(Q_arg), toImmVector(c_arg_scaled),
                   Some((toImmMatrix(A_arg), toImmVector(b_arg))),
                   Some((toImmMatrix(Aeq_arg), toImmVector(beq_arg)))),
      globalXAssignReg.map { case (k, v) => (k, v.toSeq.toMap) }.toMap,
      globalXAssignHom.map { case (k, v) => (k, v.toSeq.toMap) }.toMap)
  }

}
