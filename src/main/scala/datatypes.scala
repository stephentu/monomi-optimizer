package edu.mit.cryptdb

abstract trait DataType {
  def commonBound(that: DataType): DataType = {
    if (this == that) this else UnknownType
  }
  def toCPP: String
}

case object BoolType extends DataType {
  def toCPP = "db_elem::TYPE_BOOL"
}

case class IntType(size: Int) extends DataType {
  def toCPP = "db_elem::TYPE_INT"
  override def commonBound(that: DataType) = that match {
    case d @ DoubleType => d
    case d : DecimalType => d
    case _ => super.commonBound(that)
  }
}

case object DoubleType extends DataType {
  def toCPP = "db_elem::TYPE_DOUBLE"
  override def commonBound(that: DataType) = that match {
    case _ : IntType => this
    case _ : DecimalType => this
    case _ => super.commonBound(that)
  }
}

case class DecimalType(scale: Int, precision: Int) extends DataType {
  def toCPP = "db_elem::TYPE_DOUBLE"
  override def commonBound(that: DataType) = that match {
    case _ : IntType => this
    case d @ DoubleType => d
    case DecimalType(s, p) => DecimalType(math.max(scale, s), math.max(precision, p))
    case _ => super.commonBound(that)
  }
}

case class FixedLenString(len: Int) extends DataType {
  def toCPP = "db_elem::TYPE_STRING"
  override def commonBound(that: DataType) = that match {
    case FixedLenString(l) => FixedLenString(math.max(len, l))
    case VariableLenString(m) => VariableLenString(math.max(len, m))
    case _ => super.commonBound(that)
  }
}

case class VariableLenString(max: Int = java.lang.Integer.MAX_VALUE) extends DataType {
  def toCPP = "db_elem::TYPE_STRING"
  override def commonBound(that: DataType) = that match {
    case FixedLenString(l) => VariableLenString(math.max(max, l))
    case VariableLenString(m) => VariableLenString(math.max(max, m))
    case _ => super.commonBound(that)
  }
}

case object DateType extends DataType {
  def toCPP = "db_elem::TYPE_DATE"
}

case object IntervalType extends DataType {
  def toCPP = throw new RuntimeException("TODO")
}

case class VariableLenByteArray(max: Option[Int]) extends DataType {
  def toCPP = "db_elem::TYPE_STRING"
  override def commonBound(that: DataType) = that match {
    case VariableLenByteArray(Some(m)) => VariableLenByteArray(max.map(max => math.max(max, m)))
    case VariableLenByteArray(None)    => VariableLenByteArray(None)
    case _ => super.commonBound(that)
  }
}

// this shouldn't be necessary, but exists b/c we don't have perfect type information
// (for ex, we don't interpret the catalogue to see what UDFs return)
case object UnknownType extends DataType {
  def toCPP = throw new RuntimeException("TODO")
}

case object NullType extends DataType {
  def toCPP = "db_elem::TYPE_NULL"
}

case class TypeInfo(tpe: DataType, field: Option[(String, String)]) {
  def commonBound(that: TypeInfo): TypeInfo = {
    TypeInfo(tpe.commonBound(that.tpe), if (field == that.field) field else None)
  }
}
