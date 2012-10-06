package edu.mit.cryptdb.tpch

import edu.mit.cryptdb._
import math_defns._
import java.io.{ Console => _, _ }

object Runner {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      Console.err.println("[usage] Runner output_file cdf_file")
      Console.err.println(args.toSeq)
      System.exit(1)
    }

    val f = new File(args(0))
    val ps = new PrintStream(new FileOutputStream(f))
    scala.Console.setOut(ps)

    val queries =
      Seq(Queries.q1, Queries.q2, Queries.q3, Queries.q4, Queries.q5,
          Queries.q6, Queries.q7, Queries.q8, Queries.q9, Queries.q10,
          Queries.q11, Queries.q12, Queries.q14, Queries.q17, Queries.q18,
          Queries.q19, Queries.q20Rewritten, Queries.q21, Queries.q22)

    val _props = new java.util.Properties
    _props.setProperty("user", "stephentu")
    _props.setProperty("password", "")
    val pg = new PgSchema("localhost", 5432, "tpch_1_00", _props)

    val tester = new RobustTester
    val simulation = tester.simulate(pg, queries, None, None)

    val out = new File(args(1))
    val fps = new PrintStream(new FileOutputStream(out))
    fps.println("n = zeros(%d, 1);".format(simulation.size))
    fps.println("x = cell(%d, 1);".format(simulation.size))
    fps.println("y = cell(%d, 1);".format(simulation.size))

    simulation.toSeq.zipWithIndex.foreach { case ((i, info), idx) =>
      fps.println("n(%d) = %d;".format(idx + 1, i))
      val cdf = info.cdf
      fps.println("x{%d} = %s;".format(idx + 1, vectorAsMCode(Vector(cdf.map(_._1):_*), false)))
      fps.println("y{%d} = %s;".format(idx + 1, vectorAsMCode(Vector(cdf.map(_._2):_*), false)))
    }

    fps.flush()
    fps.close()
  }
}
