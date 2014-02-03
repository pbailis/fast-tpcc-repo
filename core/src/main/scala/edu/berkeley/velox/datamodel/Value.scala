package edu.berkeley.velox.datamodel


sealed trait Value {
  def asString : String = {
    throw new UnsupportedOperationException
  }

  def asInt : Int = {
    throw new UnsupportedOperationException
  }

  override def equals(other: Any) : Boolean
}

case class StringValue(value: String) extends AnyVal with Value {
  override def asString : String = {
    value
  }

  override def equals(other: Any) : Boolean = {
    other match {
      case StringValue(x) => x == value
      case _ => false
    }
  }
}
case class IntValue(value: Int) extends AnyVal with Value {
  override def asInt : Int = {
    value
  }

  override def equals(other: Any) : Boolean = {
    other match {
      case IntValue(x) => x == value
      case _ => false
    }
  }
}

object ValueConversions {
  implicit final def toStringValue(value: String) : StringValue = {
    StringValue(value)
  }

  implicit final def toIntValue(value: Int) : IntValue = {
    IntValue(value)
  }

  implicit final def toColumns[T](value: (String, T)) (implicit f: T => Value) : (Column, Value) = {
    (Column(value._1), f(value._2))
  }
}
