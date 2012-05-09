package edu.mit.cryptdb

import tpch._
import org.specs2.mutable._

class SQLParserSpec extends Specification {

  "SQLParser" should {
    "parse query1" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q1)
      r should beSome
    }

    "parse query2" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q2)
      r should beSome
    }

    "parse query3" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q3)
      r should beSome
    }

    "parse query4" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q4)
      r should beSome
    }

    "parse query5" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q5)
      r should beSome
    }

    "parse query6" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q6)
      r should beSome
    }

    "parse query7" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q7)
      r should beSome
    }

    "parse query8" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q8)
      r should beSome
    }

    "parse query9" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q9)
      r should beSome
    }

    "parse query10" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q10)
      r should beSome
    }

    "parse query11" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q11)
      r should beSome
    }

    "parse query12" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q12)
      r should beSome
    }

    "parse query13" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q13)
      r should beSome
    }

    "parse query14" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q14)
      r should beSome
    }

    "parse query16" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q16)
      r should beSome
    }

    "parse query17" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q17)
      r should beSome
    }

    "parse query18" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q18)
      r should beSome
    }

    "parse query19" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q19)
      r should beSome
    }

    "parse query20" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q20)
      r should beSome
    }

    "parse query21" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q21)
      r should beSome
    }

    "parse query22" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q22)
      r should beSome
    }
  }
}
