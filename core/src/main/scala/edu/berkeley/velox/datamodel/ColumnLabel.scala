package edu.berkeley.velox.datamodel

import DataModelConverters._

case class ColumnLabel(name: String, var isPrimary: Boolean=false) {

  def PRIMARY(): ColumnLabel = {
    isPrimary = true
    this
  }

  def INT() = new IntColumn(name,isPrimary)
  def STRING() = new StringColumn(name,isPrimary)

  def ===(value: Value) : EqualityPredicate = {
    EqualityPredicate(name, value)
  }
}

// typing classes for schema

trait TypedColumn extends ColumnLabel
class IntColumn(n: String, ip: Boolean) extends ColumnLabel(n,ip) with TypedColumn
class StringColumn(n: String, ip: Boolean) extends ColumnLabel(n,ip) with TypedColumn
