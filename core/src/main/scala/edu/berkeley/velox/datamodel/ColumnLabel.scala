package edu.berkeley.velox.datamodel

import DataModelConverters._

case class ColumnLabel(name: String) {
  def ===(value: Value) : EqualityPredicate = {
    EqualityPredicate(name, value)
  }
}
