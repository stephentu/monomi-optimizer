import scala.collection.mutable._
class Context(val parent: Option[Context] = None) {
  val relations = new HashMap[String, Relation]
}
