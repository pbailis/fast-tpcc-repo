package edu.berkeley.velox.datamodel

sealed trait Value {
  def asString : String = {
    throw new UnsupportedOperationException
  }

  def asInt : Int = {
    throw new UnsupportedOperationException
  }
}

// TODO: how can we get these two classes to extend AnyVal while still doing the right thing?
case class StringValue(value: String) extends Value {
  override def asString : String = {
    value
  }
}
case class IntValue(value: Int) extends Value {
  override def asInt : Int = {
    value
  }
}
