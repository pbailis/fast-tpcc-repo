package edu.berkeley.velox.datamodel

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

protected[velox] class Row {
  // TODO : this is expensive
  val columns = new ConcurrentHashMap[ColumnLabel, Value]

  def set(column: ColumnLabel, value: Value) : Row = {
    columns.put(column, value)
    this
  }

  def get(column: ColumnLabel) : Value = {
    columns.get(column)
  }

  def getColumnLabels(): Seq[ColumnLabel] = {
    columns.keys.toSeq
  }

  def project(columns: Seq[ColumnLabel]) : Row = {
    val ret = new Row()
    columns foreach {
      c => ret.set(c, this.get(c))
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

