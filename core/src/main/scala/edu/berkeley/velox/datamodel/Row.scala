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
  var updates: util.HashMap[Integer, Any] = null

  var timestamp = Timestamp.NO_TIMESTAMP
  var value: Any = null
  var transactionKeys: util.ArrayList[PrimaryKey] = null

  def this(timestamp: Long, value: Any) {
    this()
    this.timestamp = timestamp
    this.value = value
  }

  def this(timestamp: Long, value: Any, transactionKeys: util.ArrayList[PrimaryKey]) {
    this(timestamp, value)
    this.transactionKeys = transactionKeys
  }

  def column(columnName: Integer, value: Any): Row = {
    columns.put(columnName, value)
    this
  }

  def update(columnName: Integer, value: Any): Row = {
    if(updates == null)
      updates = new util.HashMap[Integer, Any]()
    updates.put(columnName, value)
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
