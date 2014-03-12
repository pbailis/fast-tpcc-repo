package edu.berkeley.velox.datamodel

trait Predicate {
  def apply(to: Value): Boolean
}

case class EqualityPredicate(column: ColumnLabel, value: Value) extends Predicate {
  def apply(to: Value): Boolean = value.equals(to)
}

case class NotEqualityPredicate(column: ColumnLabel, value: Value) extends Predicate {
  def apply(to: Value): Boolean = !value.equals(to)
}

case class LessThanPredicate(column: ColumnLabel, value: Value) extends Predicate {
  def apply(to: Value): Boolean = value.compareTo(to) > 0
}

case class LessThanEqualPredicate(column: ColumnLabel, value: Value) extends Predicate {
  def apply(to: Value): Boolean = value.compareTo(to) >= 0
}

case class GreaterThanPredicate(column: ColumnLabel, value: Value) extends Predicate {
  def apply(to: Value): Boolean = value.compareTo(to) < 0
}

case class GreaterThanEqualPredicate(column: ColumnLabel, value: Value) extends Predicate {
  def apply(to: Value): Boolean = value.compareTo(to) <= 0
}

