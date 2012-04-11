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
