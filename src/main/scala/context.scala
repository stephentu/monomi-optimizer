import scala.collection.mutable.{ArrayBuffer, HashMap}

case class Symbol(relation: String, column: String, ctx: Context)

abstract trait ProjectionType
case class NamedProjection(name: String, expr: SqlExpr) extends ProjectionType
case object WildcardProjection extends ProjectionType

class Context(val parent: Either[Definitions, Context]) {
  val relations = new HashMap[String, Relation]
  val projections = new ArrayBuffer[ProjectionType]

  def isParentOf(that: Context): Boolean = {
    var cur = that.parent.right.toOption.orNull
    while (cur ne null) {
      if (cur eq this) return true
    }
    false
  }

  // lookups first one
  def lookupProjection(name: String): Option[SqlExpr] = {
    projections.flatMap {
      case NamedProjection(n0, expr) if n0 == name => Some(expr)
      case WildcardProjection =>
        def lookupRelation(r: Relation): Option[SqlExpr] = r match {
          case TableRelation(t) =>
            defns.lookup(t, name).map(tc => {
              FieldIdent(Some(t), name, Symbol(t, name, this), this)
            })
          case SubqueryRelation(s) =>
            s.ctx.projections.flatMap {
              case NamedProjection(n0, expr) if n0 == name => Some(expr)
                
              case WildcardProjection =>
                s.ctx.lookupProjection(name)

              case _ => None
            }.headOption
        }
        relations.flatMap {
          case (_, r) => lookupRelation(r)
        }
      case _ => None
    }.headOption
  }
  
  val defns = lookupDefns()

  private def lookupDefns(): Definitions = parent match {
    case Left(d) => d
    case Right(p) => p.lookupDefns()
  }

  // finds an column in the pre-projected relation
  def lookupColumn(qual: Option[String], 
                   name: String): Seq[Symbol] = 
    lookupColumn0(qual, name, None)

  private def lookupColumn0(qual: Option[String], 
                            name: String, 
                            topLevel: Option[String]): Seq[Symbol] = {

    def lookupRelation(topLevel: String, r: Relation): Seq[Symbol] = r match {
      case TableRelation(t) =>
        defns.lookup(t, name).map(tc => Symbol(topLevel, name, this)).toSeq
      case SubqueryRelation(s) =>
        s.ctx.projections.flatMap {
          case NamedProjection(n0, expr) if n0 == name => 
            Seq(Symbol(topLevel, name, this))
          case WildcardProjection =>
            s.ctx.lookupColumn0(None, name, Some(topLevel))
          case _ => Seq.empty 
        }
    }
    val res = qual match {
      case Some(q) => 
        relations.get(q).map(x => lookupRelation(topLevel.getOrElse(q), x)).getOrElse(Seq.empty)
      case None => 
        relations.flatMap { case (r, x) => lookupRelation(topLevel.getOrElse(r), x) }.toSeq
    }

    if (res.isEmpty) {
      // lookup in parent
      parent.right.toOption.map(_.lookupColumn(qual, name)).getOrElse(Seq.empty)
    } else res
  }

  // HACK: don't use default toString(), so that built-in ast node's
  // toString can be used for structural equality
  override def toString = "Context"
}
