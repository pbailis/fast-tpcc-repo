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
  var primaryKey: Seq[ColumnLabel] = null

  def setColumns(columns: Seq[TypedColumn]): Schema = {
    this.columns = columns
    primaryKey = columns.filter(_.isPrimary)
    this
  }

  def columns(columns: TypedColumn*): Schema =
    setColumns(columns)

  override def toString: String = {
    s"(Columns: $columns)"
  }

  override def equals(that: Any): Boolean = {
    if (that.isInstanceOf[Schema])
      equals(that.asInstanceOf[Schema])
    else
      false
  }

  def equals(schema: Schema): Boolean = {
    if (schema.columns == null && columns == null)
      true
    else if (schema.columns == null || columns == null)
      false
    else if (schema.columns.size != columns.size)
      false
    else {
      schema.columns.zip(columns).foldLeft(true)(
        (eq,colpair) => {
          if (!eq) eq
          else colpair._1.schemaEquals(colpair._2)
        }
      )
    }
  }

}
