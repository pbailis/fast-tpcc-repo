package edu.berkeley.velox.datamodel

trait Predicate

case class EqualityPredicate(column: ColumnLabel, value: Value) extends Predicate
