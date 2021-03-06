package edu.berkeley.velox.datamodel

import scala.collection.mutable
import java.util.ArrayList
import java.util
import DataModelConverters._;
import scala.collection.JavaConversions._;

class InsertSet {
  private var currentInsertRow: Row = null
  private val rows: ArrayList[Row] = new ArrayList[Row]()
  private var position: Int = -1

  def absolute(position: Int) {
    if (position >= rows.size) {
      throw new UnsupportedOperationException(s"Position $position requested, but maximum position is ${rows.size-1}")
    }

    this.position = position
  }

  def currentRow(): Row = rows(position)

  def hasNext(): Boolean = {
    position+1 < rows.length
  }

  def next(): Boolean =  {
    if (position > rows.length) {
      throw new UnsupportedOperationException(s"Next requested, but maximum position is ${rows.length-1}")
    }

    position += 1
    position < rows.length
  }

  def prev() {
    if(position == 0) {
      throw new UnsupportedOperationException(s"Prev requested, but current position is at start!")
    }

    position -= 1
  }

  def beforeFirst() {

    if(rows == null) {
      throw new UnsupportedOperationException(s"No rows found in set!")
    }

    position = -1
  }

  def afterEnd() {
    if(rows == null) {
      throw new UnsupportedOperationException(s"No rows found in set!")
    }

    position = Math.max(rows.length, 1)
  }

  def getInt(at: Int): Int = {
    rows.get(position).get(at).asInstanceOf[IntValue].value
  }

  def getString(at: Int): String = {
    rows.get(position).get(at).asInstanceOf[StringValue].value
  }

  // TODO: do we want to support this operation?
  def size(): Int = {
    rows.length
  }

  def newRow(size: Int) = {
    if(currentInsertRow != null) {
      throw new UnsupportedOperationException("New row requested, but previous row was not inserted!")
    }

    currentInsertRow = new Row(size)
  }

  def insertRow = {
    if(currentInsertRow == null) {
      throw new UnsupportedOperationException("No row has been built!")
    }

    rows.add(currentInsertRow);
    currentInsertRow = null
  }

  def set(at: Int, value: Value) = {
    if(currentInsertRow == null) {
      throw new UnsupportedOperationException("No row is currently being built!")
    }

    currentInsertRow.set(at, value)
  }

  def setString(at: Int, value: String) = {
    if(currentInsertRow == null) {
      throw new UnsupportedOperationException("No row is currently being built!")
    }

    currentInsertRow.set(at, value)
  }

  def setInt(at: Int, value: Int) = {
    if(currentInsertRow == null) {
      throw new UnsupportedOperationException("No row is currently being built!")
    }

    currentInsertRow.set(at, value)
  }

  def merge(other: InsertSet) = {
    rows.addAll(other.rows)
  }

  protected[velox] def getRows: Seq[Row] = {
    rows
  }

  protected[velox] def appendRow(row: Row) {
    rows.add(row)
  }
}
