import scala.collection.mutable.{ ArrayBuffer, HashMap, Seq => MSeq }

object Onions {
  final val PLAIN        = 0x1
  final val DET          = 0x1 << 1
  final val OPE          = 0x1 << 2
  final val HOM          = 0x1 << 3
  final val HOM_ROW_DESC = 0x1 << 4
  final val HOM_AGG      = 0x1 << 5
  final val SWP          = 0x1 << 6

  // not doing HOM for now (use HOM_ROW_DESC/HOM_AGG instead)
  final val ALL          = 0x7FFFFFFF & ~HOM

  // Convenience bitmasks
  final val Countable        = PLAIN | DET | OPE
  final val Comparable       = PLAIN | DET | OPE
  final val EqualComparable  = PLAIN | DET
  final val IEqualComparable = PLAIN | OPE
  final val Searchable       = PLAIN | SWP

  def str(o: Int): String = {
    if (BitUtils.onlyOne(o)) {
      o match {
        case PLAIN        => "PLAIN"
        case DET          => "DET"
        case OPE          => "OPE"
        case HOM          => "HOM"
        case HOM_ROW_DESC => "HOM_ROW_DESC"
        case HOM_AGG      => "HOM_AGG"
        case SWP          => "SWP"
        case _            => "UNKNOWN(0x%x)".format(o)
      }
    } else toSeq(o).map(str).mkString("(", "|", ")")
  }

  def pickOne(o: Int): Int = {
    def t(m: Int) = (o & m) != 0
    if      (t(PLAIN))        PLAIN
    else if (t(DET))          DET
    else if (t(OPE))          OPE
    else if (t(HOM))          HOM
    else if (t(HOM_ROW_DESC)) HOM_ROW_DESC
    else if (t(HOM_AGG))      HOM_AGG
    else if (t(SWP))          SWP
    else throw new RuntimeException("could not pick one onion from: 0x%x".format(o))
  }

  def toSeq(o: Int): Seq[Int] = {
    val buf = new ArrayBuffer[Int]
    def t(m: Int) = (o & m) != 0
    if (t(PLAIN))        buf += PLAIN
    if (t(DET))          buf += DET
    if (t(OPE))          buf += OPE
    if (t(HOM))          buf += HOM
    if (t(HOM_ROW_DESC)) buf += HOM_ROW_DESC
    if (t(HOM_AGG))      buf += HOM_AGG
    if (t(SWP))          buf += SWP
    buf.toSeq
  }

  def completeSeqWithPreference(o: Int): Seq[Int] = {
    val s = toSeq(o)
    val s0 = s.toSet
    s ++ {
      toSeq(Onions.ALL)
       .flatMap(x => if (s0.contains(x)) Seq.empty else Seq(x)) }
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
  private val _gen = new NameGenerator("virtual")

  // string is enc name, int is Onions bitmask
  private val opts = new HashMap[(String, Either[String, SqlExpr]), (String, Int)]

  // relation -> sequence of groups
  private type HomGroup = ArrayBuffer[SqlExpr]
  private val packedHOMs = new HashMap[String, ArrayBuffer[HomGroup]]

  private def mkKey(relation: String, expr: SqlExpr) = {
    ((relation, expr match {
      case FieldIdent(_, n, _, _) => Left(n)
      case _ => Right(expr.copyWithContext(null).asInstanceOf[SqlExpr])
    }))
  }

  def getPrecomputedExprs: Map[String, SqlExpr] = {
    opts.flatMap {
      case ((_, Right(expr)), (basename, _)) => Some((basename, expr))
      case _                                 => None
    }.toMap
  }

  // relation is global table name
  def add(relation: String, expr: SqlExpr, o: Int): Unit = {
    assert((o & Onions.PLAIN) == 0)
    assert(BitUtils.onlyOne(o))
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

  // adds to previous existing group. if no groups exist, adds to last
  def addPackedHOMToLastGroup(relation: String, expr: SqlExpr): Unit = {
    val expr0 = expr.copyWithContext(null).asInstanceOf[SqlExpr]
    packedHOMs.get(relation) match {
      case Some(groups) =>
        assert(!groups.isEmpty)
        val f = groups.last.toSet
        if (!f.contains(expr0)) groups.last += expr0
      case None =>
        packedHOMs.put(relation, ArrayBuffer(ArrayBuffer(expr0)))
    }
  }

  // return value is:
  // (group number (unique per relation), in group position (unique per group))
  def lookupPackedHOM(relation: String, expr: SqlExpr): Seq[(Int, Int)] = {
    val expr0 = expr.copyWithContext(null).asInstanceOf[SqlExpr]
    packedHOMs.get(relation).map { _.zipWithIndex.flatMap { case (group, gidx) =>
      group.zipWithIndex.filter { _._1 == expr0 }.map { case (_, pidx) => (gidx, pidx) }
    }}.getOrElse(Seq.empty)
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
    merged.packedHOMs ++= packedHOMs
    that.packedHOMs.foreach {
      case (k, v) =>
        merged.packedHOMs.get(k) match {
          case Some(v0) => merged.packedHOMs.put(k, v ++ v0)
          case None => merged.packedHOMs.put(k, v)
        }
    }
    merged
  }

  def complete(d: Definitions): OnionSet = {
    val m = new OnionSet
    m.opts ++= opts
    m.packedHOMs ++= packedHOMs
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
    packedHOMs.foreach {
      case (k, vs) =>
        cpy.packedHOMs.put(k, vs.map(_.map(_.copyWithContext(null).asInstanceOf[SqlExpr])))
    }
    cpy
  }

  def isEmpty: Boolean = opts.isEmpty && packedHOMs.isEmpty

  override def toString =
    "OnionSet(opts = " + opts.toString +
    ", packedHOMs = " + packedHOMs.toString + ")"
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
