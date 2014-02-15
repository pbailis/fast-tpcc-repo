package edu.berkeley.velox.datamodel

import DataModelConverters._

case class ColumnLabel(name: String) extends AnyVal {
  def ===(value: Value) : EqualityPredicate = {
    EqualityPredicate(name, value)
  }
}
