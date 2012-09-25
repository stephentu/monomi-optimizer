package edu.mit.cryptdb

import java.sql._
import java.io._
import scala.collection.mutable.{ ArrayBuffer, HashSet }

object Conversions {
  implicit def rsWrap(rs: ResultSet) = new ResultSetWrapper(rs)
}

class ResultSetWrapper(rs: ResultSet) {
  // requires the results to fit in memory
  def map[A](f: ResultSet => A): Seq[A] = {
    val buf = new ArrayBuffer[A]
    try {
      while (rs.next()) {
        buf += f(rs)
      }
    } finally { rs.close() }
    buf.toSeq
  }

  @inline
  def foreach[U](f: ResultSet => U): Unit = map(f)
}

// NOT THREAD SAFE
class NameGenerator(val prefix: String) {
  private var _ctr: Long = 0L
  def uniqueId(): String = {
    val ret = prefix + _ctr
    _ctr += 1
    ret
  }
}

class ThreadSafeNameGenerator(val prefix: String) {
  private val _ctr = new java.util.concurrent.atomic.AtomicLong
  def uniqueId(): String = {
    prefix + _ctr.getAndIncrement().toString
  }
}

trait FunctionUtils {
  def wrapReturnTrue[B, A](f: B => A): B => Boolean = (n: B) => { f(n); true }
}

object CollectionUtils {
  def powerSet[T](s: Seq[T]): Seq[Seq[T]] = {
    if (s.isEmpty) return Seq.empty
    if (s.size == 1) return Seq(Seq(s.head), Seq.empty)
    powerSet(s.tail).flatMap(x => {
      Seq(Seq(s.head) ++ x, x)
    })
  }

  def powerSetMinusEmpty[T](s: Seq[T]): Seq[Seq[T]] =
    powerSet(s).filterNot(_.isEmpty)

  def allPossibleGroupings[T](s: Set[T]): Set[ Set[Set[T]] ] = {

    def recur(elems: Set[T], buckets: Int): Set[ Set[Set[T]] ] = {
      assert(!elems.isEmpty)
      assert(buckets >= 1)
      assert(buckets <= elems.size)

      if (buckets == 1) {
        Set( Set( elems ) )
      } else {
        val maxElemsInBucket = elems.size - (buckets - 1)

        // generate powerset of the elems, and only consider
        // those (unique) sets with <= maxElemsInBucket
        val ps =
          powerSetMinusEmpty(elems.toSeq).map(_.toSet).toSet.filter(_.size <= maxElemsInBucket)

        ps.flatMap { s =>
          val remaining = elems -- s
          val x = recur(remaining, buckets - 1)
          x.map { _ ++ Set(s) }
        }
      }
    }

    (1 to s.size).flatMap(b => recur(s, b)).toSet
  }

  def crossProduct[A, B](as: Seq[A], bs: Seq[B]): Seq[(A, B)] = {
    for (a <- as; b <- bs) yield (a, b)
  }

  def optAnd2[T0, T1](t0: Option[T0], t1: => Option[T1]): Option[(T0, T1)] = {
    if (t0.isDefined) {
      t1.map(x => (t0.get, x))
    } else None
  }

  def optOrEither2[T0, T1](t0: Option[T0], t1: => Option[T1]): Option[Either[T0, T1]] = {
    if (t0.isDefined) Some(Left(t0.get))
    else if (t1.isDefined) Some(Right(t1.get))
    else None
  }

  def optSeq[T](s: Seq[Option[T]]): Option[Seq[T]] = {
    val s0 = s.flatten
    if (s0.size == s.size) Some(s0) else None
  }

  def uniqueInOrder[A](xs: Seq[A]): Seq[A] = {
    uniqueInOrderWithKey(xs)((x: A) => x)
  }

  def uniqueInOrderWithKey[A, B](xs: Seq[A])(k: A => B): Seq[A] = {
    val s = new HashSet[B]
    xs.flatMap { case x =>
      val key = k(x)
      if (s.contains(key)) None else {
        s += key
        Some(x)
      }
    }
  }

  def allIndicesOf(s: String, c: Char): Seq[Int] = {
    s.zipWithIndex.foldLeft( Seq.empty : Seq[Int] ) {
      case (xs, (c0, i)) => if (c == c0) xs ++ Seq(i) else xs
    }
  }

  def foreachWithAllButLastAction[A, B, C](xs: Seq[A])(f: A => B)(action: () => C): Unit = {
    val s = xs.size
    xs.zipWithIndex.foreach { case (elem, idx) =>
      f(elem)
      if ((idx + 1) != s) action()
    }
  }

  // [xs[idxs[0]], xs[idxs[1]], ...]
  def slice[A](xs: Seq[A], idxs: Seq[Int]): Seq[A] = {
    idxs.map(i => xs(i))
  }
}

object BitUtils {
  def onlyOne(m: Int) = (m != 0) && (m & (m-1)) == 0
}

trait PrettyPrinters {
  // TODO: better escaping
  protected def quoteSingle(s: String): String = "'" + s.replaceAll("'", "\\\\'") + "'"
  protected def quoteDbl(s: String): String = "\"" + s.replaceAll("\"", "\\\\\"") + "\""
}

trait Timer {
  def timedRunMillis[A](f: => A): (Double, A) = {
    val start = System.nanoTime
    val ret = f
    val end = System.nanoTime
    val ms = ((end - start).toDouble / 1000000.0)
    (ms, ret)
  }

  def timedRunMillisNoReturn[A](f: => A): Double = {
    val (t, _) = timedRunMillis(f)
    t
  }
}

object ProcUtils {
  // TODO: there's definitely a more "idiomatic" way to do this
  // in Scala!
  def execCommandWithResults(cmd: String): Seq[String] = {
    val p = Runtime.getRuntime.exec(cmd)
    val br = new BufferedReader(new InputStreamReader(p.getInputStream))
    var line = br.readLine()
    val ret = new ArrayBuffer[String]
    while (line ne null) {
      ret += line
      line = br.readLine()
    }
    br.close()
    ret
  }
}

// assumes object's hashCode is deterministic and value isn't changing
final class CacheProxy[T](val value: T) {
  private var _h: Int = 0
  override def hashCode = {
    // java memory model makes this thread-safe
    if (_h == 0)
      _h = value.hashCode
    _h
  }
  override def equals(that: Any): Boolean = {
    if (this eq that.asInstanceOf[AnyRef])
      return true
    if (!that.isInstanceOf[CacheProxy[_]])
      return false
    value.equals(that.asInstanceOf[CacheProxy[_]].value)
  }
}
