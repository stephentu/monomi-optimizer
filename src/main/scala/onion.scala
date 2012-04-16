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
  private val opts = new HashMap[(String, Either[String, SqlExpr]), (String, Int)] 

  private def mkKey(relation: String, expr: SqlExpr) = {
    val TableRelation(name) = expr.ctx.relations(relation)
    ((name, expr match {
      case FieldIdent(_, n, _, _) => Left(n)
      case _ => Right(expr.copyWithContext(null).asInstanceOf[SqlExpr]) 
    }))
  }

  def add(relation: String, expr: SqlExpr, o: Int): Unit = {
    val key = mkKey(relation, expr)
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

  def lookup(relation: String, expr: SqlExpr): Option[(String, Int)] = {
    val key = mkKey(relation, expr)
    opts.get(key)
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

  def complete(d: Definitions): OnionSet = {
    val m = new OnionSet
    m.opts ++= opts
    d.defns.foreach {
      case (relation, columns) =>
        columns.foreach(tc => {
          val key = (relation, Left(tc.name))
          m.opts.get(key) match {
            case Some((v1, v2)) =>
              if ((v2 & Onions.DET) == 0 &&
                  (v2 & Onions.OPE) == 0) {
                m.opts.put(key, (v1, v2 | Onions.DET))
              }
            case None => m.opts.put(key, (tc.name, Onions.DET))
          }
        })
    }
    m
  }

  override def toString = "OnionSet(" + opts.toString + ")"
}
