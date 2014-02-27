package edu.berkeley.velox.datamodel

import DataModelConverters._

case class ColumnLabel(name: String, var isPrimary: Boolean=false) {

  def primary(): ColumnLabel = {
    isPrimary = true
    this
  }

  def INT() = new IntColumn(name,isPrimary)
  def STRING() = new StringColumn(name,isPrimary)

  def ===(value: Value) : EqualityPredicate = {
    EqualityPredicate(name, value)
  }

  override def equals(that: Any): Boolean = {
    if (that.isInstanceOf[ColumnLabel]) {
      val cl = that.asInstanceOf[ColumnLabel]
      cl.name == name && cl.isPrimary == isPrimary
    }
    else false // not same type
  }

}

// typing classes for schema

trait TypedColumn extends ColumnLabel
class IntColumn(n: String, ip: Boolean) extends ColumnLabel(n,ip) with TypedColumn {
  override def equals(that: Any): Boolean = {
    if (that.isInstanceOf[IntColumn]) {
      val ic = that.asInstanceOf[IntColumn]
      ic.name == name && ic.isPrimary == isPrimary
    }
    else false // not same type
  }
}
class StringColumn(n: String, ip: Boolean) extends ColumnLabel(n,ip) with TypedColumn {
  override def equals(that: Any): Boolean = {
    if (that.isInstanceOf[StringColumn]) {
      val sc = that.asInstanceOf[StringColumn]
      sc.name == name && sc.isPrimary == isPrimary
    }
    else false // not same type
  }
}
