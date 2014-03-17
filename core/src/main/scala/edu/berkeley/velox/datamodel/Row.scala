package edu.berkeley.velox.datamodel

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.{Buffer,StringBuilder}

protected[velox] class Row(val values: Array[Value]) {

  def this(width: Int) = this(new Array[Value](width))

  // If this is a split row, how many columns come before me
  private var offset = 0

  def set(at: Int, value: Value) : Row = {
    values(at-offset) = value
    this
  }

  def get(at: Int) : Value = {
    values(at-offset)
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

  def projectInto(ats: Seq[Int], buf: Buffer[Value]) {
    var i = 0
    while (i < ats.length) {
      if (ats(i) >= offset) // ignore cols less than my offset
        buf += get(ats(i))
      i+=1
    }
  }

  /** Test if this row passes all the specified predicates.
    *
    * @param predicates A sequence of predicates to be tested.
    * @returns true if ALL the specified predicates match, otherwise false
    */
  def matches(predicates: Seq[Predicate]): Boolean = {
    var i = 0
    while (i < predicates.size) {
      if ( predicates(i).columnIndex >= offset &&
           !predicates(i)(values(predicates(i).columnIndex-offset)) )
        return false
      i+=1
    }
    true
  }

  /** Split the row at the specified point into a primary key
    * and the remaining row.
    *
    * @param i The number of columns that should be considered
    *          the primary key
    *
    * If the remaining row would be empty Row.nullRow is returned for that side.
    */
  def splitIntoKeyVal(i: Int):(PrimaryKey,Row) = {
    val (v1,v2) = values.splitAt(i)
    val key =
      if (v1.length > 0) PrimaryKey(v1)
      else throw new Exception("Primary key cannot be null")
    val rest =
      if (v2.length > 0) new Row(v2)
      else Row.nullRow
    rest.offset = v1.length
    (key,rest)
  }

  override def toString(): String = {
    val sb = new StringBuilder("row(")
    sb.append(values.mkString(","))
    sb.append(")")
    sb.result
  }
}

object Row {
  // use this when there is no row data
  // so we only store a reference and
  // don't make new objects
  val nullRow = new Row(0)
}
