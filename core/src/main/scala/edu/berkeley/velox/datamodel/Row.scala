package edu.berkeley.velox.datamodel

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.StringBuilder

protected[velox] class Row(width: Int) {
  // TODO : this is expensive
  val values = new Array[Value](width)

  def set(at: Int, value: Value) : Row = {
    values(at) = value
    this
  }

  def get(at: Int) : Value = {
    values(at)
  }

  def project(ats: Seq[Int]) : Row = {
    val ret = new Row(ats.size)
    var i = 0
    while (i < ats.length) {
      ret.set(i, this.get(ats(i)))
      i+=1
    }
    ret
  }

  override def toString(): String = {
    val sb = new StringBuilder("row(")
    sb.append(values.mkString(","))
    sb.append(")")
    sb.result
  }
}

