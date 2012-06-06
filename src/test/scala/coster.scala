package edu.mit.cryptdb

import tpch._
import org.specs2.mutable._

class CosterSpec extends Specification with SchemaInit {

  object resolver extends Resolver
  object generator extends Generator
  object coster extends Coster

  private def doTest(q: String) = {
    val parser = new SQLParser
    val r = parser.parse(q)
    val pg = init()
    val schema = pg.loadSchema()
    val stats = pg.loadStats()
    val s0 = resolver.resolve(r.get, schema)
    val plans = generator.generateCandidatePlans(s0)
    plans must not be empty
    coster.costPlan(plans,stats)
  }

  "Coster" should {
    "cost plans for query1" in {
      doTest(Queries.q1) must not be empty
    }

    "cost plans for query2" in {
      doTest(Queries.q2) must not be empty
    }

    "cost plans for query3" in {
      doTest(Queries.q3) must not be empty
    }

    "cost plans for query4" in {
      doTest(Queries.q4) must not be empty
    }

    "cost plans for query5" in {
      doTest(Queries.q5) must not be empty
    }

    "cost plans for query6" in {
      doTest(Queries.q6) must not be empty
    }

    "cost plans for query7" in {
      doTest(Queries.q7) must not be empty
    }

    "cost plans for query8" in {
      doTest(Queries.q8) must not be empty
    }

    "cost plans for query9" in {
      doTest(Queries.q9) must not be empty
    }

    "cost plans for query10" in {
      doTest(Queries.q10) must not be empty
    }

    "cost plans for query11" in {
      doTest(Queries.q11) must not be empty
    }

    "cost plans for query12" in {
      doTest(Queries.q12) must not be empty
    }

    "cost plans for query13" in {
      doTest(Queries.q13) must not be empty
    }

    "cost plans for query14" in {
      doTest(Queries.q14) must not be empty
    }

    "cost plans for query16" in {
      doTest(Queries.q16) must not be empty
    }

    "cost plans for query17" in {
      doTest(Queries.q17) must not be empty
    }

    "cost plans for query18" in {
      doTest(Queries.q18) must not be empty
    }

    "cost plans for query19" in {
      doTest(Queries.q19) must not be empty
    }

    "cost plans for query20" in {
      doTest(Queries.q20) must not be empty
    }

    "cost plans for query21" in {
      doTest(Queries.q21) must not be empty
    }

    "cost plans for query22" in {
      doTest(Queries.q22) must not be empty
    }
  }

}

class MultiCosterSpec extends Specification with SchemaInit {

  object resolver extends Resolver
  object generator extends Generator
  object coster extends Coster

  "MultiGenerator" should {
    "cost plans for all of tpch queries" in {
      val queries = Queries.AllQueries
      val p = new SQLParser
      val unresolved = queries.map(p.parse(_))
      val pg = init()
      val schema = pg.loadSchema()
      val stats = pg.loadStats()
      val resolved = unresolved.map(x => resolver.resolve(x.get, schema))
      val plans = generator.generateCandidatePlans(resolved)
      plans.size must_== queries.size
      coster.costPlans(plans, stats) must_== queries.size
    }
  }
}
