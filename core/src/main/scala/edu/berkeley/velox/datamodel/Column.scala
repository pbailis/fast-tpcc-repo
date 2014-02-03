package edu.berkeley.velox.datamodel

case class Column(name: String) extends AnyVal {
  def ~=(value: Value) : EqualityPredicate = {
    EqualityPredicate(name, value)
  }
}
