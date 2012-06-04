package edu.mit.cryptdb

import java.util.{ Calendar, Date => JUtilDate }

trait DbElem {
  def toAST: SqlExpr
}

case object NullElem extends DbElem {
  def toAST = NullLiteral()
}

case class IntElem(v: Long) extends DbElem {
  def toAST = IntLiteral(v)
}

case class DoubleElem(v: Double) extends DbElem {
  def toAST = FloatLiteral(v)
}

case class StringElem(v: String) extends DbElem {
  def toAST = StringLiteral(v)
}

case class DateElem(v: JUtilDate) extends DbElem {

  def toAST = {
    // java date objects are annoying
    val c = Calendar.getInstance
    c.setTime(v)
    DateLiteral(
      "%d-%d-%d".format(
        c.get(Calendar.YEAR),  
        c.get(Calendar.MONTH) + 1,
        c.get(Calendar.DATE)))
  }

  def addInterval(i: IntervalElem): DateElem = {
    val c = Calendar.getInstance
    c.setTime(v)
    val flag = i.tpe match {
      case DAY   => Calendar.DATE
      case MONTH => Calendar.MONTH
      case YEAR  => Calendar.YEAR
    }
    c.add(flag, i.v)
    DateElem(c.getTime)
  }

  def subtractInterval(i: IntervalElem): DateElem = {
    addInterval(i.copy(v = -i.v))
  }

}

case class IntervalElem(v: Int, tpe: ExtractType) extends DbElem {
  def toAST = IntervalLiteral(v.toString, tpe)
}
