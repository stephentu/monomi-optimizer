import java.sql._
import scala.collection.mutable.ArrayBuffer

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
}

// NOT THREAD SAFE
class NameGenerator(prefix: String, reserved: String = "$") {
  private var _ctr: Long = 0L
  def uniqueId(): String = {
    val ret = prefix + reserved + _ctr
    _ctr += 1
    ret
  }
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
}
