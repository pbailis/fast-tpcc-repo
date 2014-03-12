package edu.berkeley.velox.datamodel

trait Value extends Any {
  def asString: String = throw new UnsupportedOperationException
  def asInt: Int = throw new UnsupportedOperationException
  def compareTo(other: Value): Int = throw new UnsupportedOperationException
}

case class StringValue(val value: String) extends AnyVal with Value {
  override def asString : String = {
    value
  }

  override def compareTo(other: Value): Int = {
    val s = other.asString
    value.compareTo(s)
  }
}
case class IntValue(val value: Int) extends AnyVal with Value {
  override def asInt : Int = {
    value
  }

  override def compareTo(other: Value): Int = {
    val i = other.asInt
    value.compareTo(i)
  }
}
