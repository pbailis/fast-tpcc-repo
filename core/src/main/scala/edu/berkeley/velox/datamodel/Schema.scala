package edu.berkeley.velox.datamodel

import scala.collection.mutable.ArrayBuilder
import scala.collection.immutable.HashMap

object Schema {
  def columns(columns: TypedColumn*): Schema = {
    val ret = new Schema
    ret.setColumns(columns)
    ret
  }
}

class Schema {
  var columns: Array[TypedColumn] = null
  var numPkCols = 0
  // maps index names to index definition (Array of TypedColumn's)
  var indexes = new HashMap[String, Array[TypedColumn]]

  def setColumns(columns: Seq[TypedColumn]): Schema = {
    val bldr = new ArrayBuilder.ofRef[TypedColumn]
    bldr.sizeHint(columns.size)
    numPkCols = 0

    columns.foreach(col =>
      {
        if (col.isPrimary) {
          numPkCols+=1
          bldr += col
        }
      })

    columns.foreach(col =>
      {
        if (!col.isPrimary)
          bldr += col
      })


    this.columns = bldr.result
    this
  }

  def columns(columns: TypedColumn*): Schema =
    setColumns(columns)

  def index(indexName: String, indexColumns: String*): Schema = {
    val bldr = new ArrayBuilder.ofRef[TypedColumn]
    bldr.sizeHint(columns.size)
    indexColumns.foreach(columnName => {
      // Search for the TypedColumn object in the table schema
      val index = indexOf(new ColumnLabel(columnName))
      if (index == -1) {
        // Index column not found in table!
        throw new IllegalStateException(s"index.column: $indexName.$columnName does not exist in table.")
      }
      val col = columns(index).copy()
      col.isPrimary = true
      bldr += col
    })
    indexes += ((indexName, bldr.result))
    this
  }

  def indexOf(column: ColumnLabel): Int = {
    var i = 0
    while (i < columns.length) {
      if (columns(i).equals(column))
        return i
      i+=1
    }
    -1 // not found
  }

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
