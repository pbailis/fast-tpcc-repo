package edu.berkeley.velox.datamodel

abstract class Predicate(val column: ColumnLabel) {
  var columnIndex = -1
  def apply(to: Value): Boolean
}

case class EqualityPredicate(override val column: ColumnLabel, value: Value) extends Predicate(column) {
  def apply(to: Value): Boolean = value.equals(to)
}

case class NotEqualityPredicate(override val column: ColumnLabel, value: Value) extends Predicate(column) {
  def apply(to: Value): Boolean = !value.equals(to)
}

case class LessThanPredicate(override val column: ColumnLabel, value: Value) extends Predicate(column) {
  def apply(to: Value): Boolean = value.compareTo(to) > 0
}

case class LessThanEqualPredicate(override val column: ColumnLabel, value: Value) extends Predicate(column) {
  def apply(to: Value): Boolean = value.compareTo(to) >= 0
}

case class GreaterThanPredicate(override val column: ColumnLabel, value: Value) extends Predicate(column) {
  def apply(to: Value): Boolean = value.compareTo(to) < 0
}

case class GreaterThanEqualPredicate(override val column: ColumnLabel, value: Value) extends Predicate(column) {
  def apply(to: Value): Boolean = value.compareTo(to) <= 0
}
