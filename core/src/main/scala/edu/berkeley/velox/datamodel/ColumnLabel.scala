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

  // Use just the name so we don't have to set primary every time we
  // want to use a column name as a key in a hashtable
  override def hashCode: Int = name.hashCode

  override def equals(that: Any): Boolean = {
    if (that.isInstanceOf[ColumnLabel]) {
      val cl = that.asInstanceOf[ColumnLabel]
      // only check name equality for hash key suitability
      cl.name == name
    }
    else false // not same type
  }

}

// typing classes for schema

trait TypedColumn extends ColumnLabel {
  // check for equality from the perspective of the schema
  // does include exact type and isPrimary checks
  def schemaEquals(col: TypedColumn): Boolean
}
class IntColumn(n: String, ip: Boolean) extends ColumnLabel(n,ip) with TypedColumn {
  override def schemaEquals(col: TypedColumn): Boolean = {
    if (col.isInstanceOf[IntColumn]) {
      val ic = col.asInstanceOf[IntColumn]
      ic.name == name && ic.isPrimary == isPrimary
    }
    else false // not same type
  }
}
class StringColumn(n: String, ip: Boolean) extends ColumnLabel(n,ip) with TypedColumn {
  override def schemaEquals(col: TypedColumn): Boolean = {
    if (col.isInstanceOf[StringColumn]) {
      val sc = col.asInstanceOf[StringColumn]
      sc.name == name && sc.isPrimary == isPrimary
    }
    else false // not same type
  }
}
