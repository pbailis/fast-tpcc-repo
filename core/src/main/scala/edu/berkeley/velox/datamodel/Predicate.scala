package edu.berkeley.velox.datamodel

trait Predicate {}

case class EqualityPredicate(column: Column, value: Value) extends Predicate
