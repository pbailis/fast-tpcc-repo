package edu.berkeley.kaiju.storedproc.datamodel

import java.util.Collection
import java.util.Map
import java.util

object Row {
  def pkey(pkeys: Int*): Row = {
    return new Row(pkeys.toArray)
  }
}

class Row {
  def this(keys: Array[Int]) {
    this()
    this.keys = keys
    this.values = new util.HashMap[Integer, AnyRef]
  }

  def column(column: Int, value: Any): Row = {
    this.columnRef(column, value.asInstanceOf[AnyRef])
  }

  def columnRef(column: Int, value: AnyRef): Row = {
    values.put(column, value)
    return this
  }

  def column(column: Int): Row = {
    values.put(column, null)
    return this
  }

  def readColumn(columnName: Int): AnyRef = {
    return values.get(columnName)
  }

  def numColumns: Int = {
    return keys.length + 1
  }

  def getKeys: Array[Int] = {
    return keys
  }

  def getColumns: Collection[Integer] = {
    return values.keySet
  }

  def getValue(column: Int): Object = {
    return values.get(column)
  }

  private var keys: Array[Int] = null
  private var values: Map[Integer, Object] = null
}

