import scala.collection.mutable._
class Context(val parent: Option[Context] = None) {
  val relations = new HashMap[String, Relation]
  val projections = new ArrayBuffer[Column]

  def lookupColumn(qual: Option[String], name: String): Option[Column] = 
    qual match {
      case Some(tbl) => relations.get(tbl).flatMap(_.lookupColumn(name)).orElse(
          parent.flatMap(_.lookupColumn(qual, name)))
      case None => 
        relations.values.flatMap(_.columns).filter(_.name == name).headOption.orElse(
          parent.flatMap(_.lookupColumn(qual, name)))
    }

  // HACK: don't use default toString(), so that built-in ast node's
  // toString can be used for structural equality
  override def toString = "Context"
}
