package edu.berkeley.velox.datamodel

trait Value extends Any {
  def asString : String = throw new UnsupportedOperationException
  def asInt : Int = throw new UnsupportedOperationException
}

case class StringValue(val value: String) extends AnyVal with Value {
  override def asString : String = {
    value
  }
}
case class IntValue(val value: Int) extends AnyVal with Value {
  override def asInt : Int = {
    value
  }
}
