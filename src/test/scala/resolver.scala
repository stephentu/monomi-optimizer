import org.specs2.mutable._

class ResolverSpec extends Specification {

  object resolver extends Resolver

  "Resolver" should {
    "resolve query1" in {
      val parser = new SQLParser
      val r = parser.parse(Queries.q1)
      val s0 = resolver.resolve(r.get, TestSchema.definition)
      s0.ctx.projections.size must_== 10
    }
  }
}
