import scala.collection.mutable.HashMap

object Onions {
  final val DET = 0x1
  final val OPE = 0x1 << 1
  final val HOM = 0x1 << 2
  final val SWP = 0x1 << 3
  final val ALL = 0x7FFFFFFF

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
    ((relation, expr match {
      case FieldIdent(_, n, _, _) => Left(n)
      case _ => Right(expr.copyWithContext(null).asInstanceOf[SqlExpr])
    }))
  }

  // relation is global table name
  def add(relation: String, expr: SqlExpr, o: Int): Unit = {
    val key = mkKey(relation, expr)
    opts.get(key) match {
      case Some((v1, v2)) =>
        opts.put(key, (v1, v2 | o))
      case None =>
        opts.put(key, (expr match {
          case FieldIdent(_, name, _, _) => name
          case _ => _gen.uniqueId()
          }, o))
    }
  }

  // relation is global table name
  def lookup(relation: String, expr: SqlExpr): Option[(String, Int)] = {
    opts.get(mkKey(relation, expr))
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

  // deep copy
  def copy: OnionSet = {
    val cpy = new OnionSet
    opts.foreach {
      case (k @ (relation, Left(name)), v) =>
        cpy.opts.put(k, v)
      case ((relation, Right(expr)), v) =>
        cpy.opts.put((relation, Right(expr.copyWithContext(null).asInstanceOf[SqlExpr])), v)
    }
    cpy
  }

  def isEmpty: Boolean = opts.isEmpty

  override def toString = "OnionSet(" + opts.toString + ")"
}

// this is a necessary evil until we rework the transformers api
// not thread safe
class SetOnce[T] {
  private var _value: Option[T] = None
  def set(v: T): Unit = {
    if (!_value.isDefined) _value = Some(v)
  }
  def get: Option[T] = _value
}
