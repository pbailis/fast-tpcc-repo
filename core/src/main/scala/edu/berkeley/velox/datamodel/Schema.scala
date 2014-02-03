package edu.berkeley.velox.datamodel

object Schema {
  def pkey(pkey: PrimaryKeyDefinition): Schema = {
    val ret = new Schema
    ret.pkey(pkey)
    ret
  }

  def columns(columns: (ColumnLabel , Value)*): Schema = {
    val ret = new Schema
    ret.setColumns(columns)
    ret
  }
}

class Schema {
  var pkey: PrimaryKeyDefinition = null
  var columns: Seq[(ColumnLabel, Value)] = null

  def pkey(pkey: PrimaryKeyDefinition): Schema = {
    this.pkey = pkey
    this
  }

  def setColumns(columns: Seq[(ColumnLabel, Value)]): Schema = {
    this.columns = columns
    this
  }

  def columns(columns: (ColumnLabel, Value)*): Schema = {
    this.columns = columns
    this
  }

  override def toString: String = {
    s"(PrimaryKey: $pkey, Columns: $columns)"
  }
}

case class PrimaryKeyDefinition(val value: Seq[ColumnLabel])
