import scala.collection.mutable.HashMap

object Onions {
  final val DET = 0x1
  final val OPE = 0x1 << 1
  final val HOM = 0x1 << 2
  final val SWP = 0x1 << 3

  def str(o: Int): String = o match {
    case DET => "DET"
    case OPE => "OPE"
    case HOM => "HOM"
    case SWP => "SWP"
  }
}

object OnionSet {
  def merge(sets: Seq[OnionSet]): OnionSet = {
    sets.foldLeft(new OnionSet) { 
      case (acc, elem) => acc.merge(elem)
    }
  }
}

class OnionSet {
  private val _gen = new NameGenerator("_virtual")

  // string is enc name, int is Onions bitmask
  val opts = new HashMap[(String, SqlExpr), (String, Int)] 

  def add(relation: String, expr: SqlExpr, o: Int): Unit = {
    val relation0 = expr.ctx.relations(relation)
    assert(relation0.isInstanceOf[TableRelation])

    val key = ((relation0.asInstanceOf[TableRelation].name, expr))
    opts.get(key) match {
      case Some((v1, v2)) => 
        opts.put(key, (v1, v2 | o))
      case None =>
        opts.put(key, (expr match {
          // TODO: not general enough
          case FieldIdent(qual, name, _, _) => name
          case _ => _gen.uniqueId()
          }, o))
    }
  }

  def merge(that: OnionSet): OnionSet = {
    val merged = new OnionSet
    merged.opts ++= opts
    that.opts.foreach {
      case (k, v @ (v1, v2)) =>
        merged.opts.get(k) match {
          case Some((ov1, ov2)) => merged.opts.put(k, (ov1, ov2 | v2))
          case None => merged.opts.put(k, v)
        }
    }
    merged
  }
  override def toString = "OnionSet(" + opts.toString + ")"
}
