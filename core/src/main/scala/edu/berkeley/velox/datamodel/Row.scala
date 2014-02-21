package edu.berkeley.velox.datamodel

import java.util.Collection
import java.util

object Row {
  val NULL = new Row

  def column(column: Int, value: Any): Row = {
    val ret = new Row
    ret.column(column, value)
    ret
  }

  def column(column: Int): Row = {
    val ret = new Row
    ret.column(column)
    ret
  }
}

class Row {
  val columns = new util.HashMap[Integer, Any]

  var timestamp = Timestamp.NO_TIMESTAMP
  var transactionKeys: Array[PrimaryKey] = null

  def column(columnName: Integer, value: Any): Row = {
    columns.put(columnName, value)
    this
  }

  def column(columnName: Integer): Row = {
    columns.put(columnName, null)
    this
  }

  def readColumn(columnName: Integer): Any = {
    columns.get(columnName)
  }
}
