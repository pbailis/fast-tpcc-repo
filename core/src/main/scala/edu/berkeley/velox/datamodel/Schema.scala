package edu.berkeley.velox.datamodel

object Schema {
  def columns(columns: TypedColumn*): Schema = {
    val ret = new Schema
    ret.setColumns(columns)
    ret
  }
}

class Schema {
  var columns: Seq[TypedColumn] = null

  def setColumns(columns: Seq[TypedColumn]): Schema = {
    this.columns = columns
    this
  }

  def columns(columns: TypedColumn*): Schema = {
    this.columns = columns
    this
  }

  override def toString: String = {
    s"(Columns: $columns)"
  }
}
