package edu.berkeley.velox.datamodel

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

class Row {
  // TODO : this is expensive
  val columns = new ConcurrentHashMap[Column, Value]

  def column(column: Column, value: Value) : Row = {
    columns.put(column, value)
    this
  }

  def column(column: Column) : Value = {
    columns.get(column)
  }

  def project(columns: Seq[Column]) : Row = {
    val ret = new Row()
    columns foreach {
      c => ret.column(c, this.column(c))
    }
    ret
  }

  def hashValues: Int = {
    columns.values.asScala.foldLeft(1)((b, a) => b*a.hashCode())
  }

  override def hashCode: Int = {
    var ret = 0
    columns.entrySet.asScala.foreach(pair => ret *= pair.getKey.name.hashCode*pair.getValue.hashCode())
    ret
  }
}

object RowConversion {
  implicit final def toRow(values: Seq[(Column, Value)]): Row = {
    val row = new Row
    values.foreach { case (c: Column, v: Value) => row.column(c, v) }
    row
  }
}

