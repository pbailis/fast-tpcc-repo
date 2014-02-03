package edu.berkeley.velox.datamodel

import scala.collection.mutable
import java.util.ArrayList
import java.util
import DataModelConverters._;
import scala.collection.JavaConversions._;

class InsertSet {
  private var currentInsertRow: Row = null
  private val rows: ArrayList[Row] = new ArrayList[Row]()
  private var position: Int = 0

  def absolute(position: Int) {
    if (position > rows.size-1) {
      throw new UnsupportedOperationException(s"Position $position requested, but maximum position is ${rows.size-1}")
    }

    this.position = position
  }

  def hasNext(): Boolean = {
    position+1 < rows.size()
  }

  def next() {
    if(position+1 > rows.size) {
      throw new UnsupportedOperationException(s"Next requested, but maximum position is ${rows.size-1}")
    }

    position += 1
  }

  def prev() {
    if(position == 0) {
      throw new UnsupportedOperationException(s"Prev requested, but current position is at start!")
    }

    position -= 1
  }

  def start() {
    if(rows == null) {
      throw new UnsupportedOperationException(s"No rows found in set!")
    }

    position = 0
  }

  def end() {
    if(rows == null) {
      throw new UnsupportedOperationException(s"No rows found in set!")
    }

    position = Math.max(rows.size()-1, 0)
  }

  def getInt(columnName: ColumnLabel): Int = {
    rows.get(position).get(columnName).asInstanceOf[IntValue].value
  }

  def getString(columnName: ColumnLabel): String = {
    rows.get(position).get(columnName).asInstanceOf[StringValue].value
  }

  def getColumnLabels(): Seq[ColumnLabel] = {
    rows.get(position).getColumnLabels
  }

  // TODO: do we want to support this operation?
  def size(): Int = {
    rows.size()
  }

  def newRow = {
    if(currentInsertRow != null) {
      throw new UnsupportedOperationException("New row requested, but previous row was not inserted!")
    }

    currentInsertRow = new Row
  }

  def insertRow = {
    if(currentInsertRow == null) {
      throw new UnsupportedOperationException("No row has been built!")
    }

    rows.add(currentInsertRow);
    currentInsertRow = null
  }

  def set(column: ColumnLabel, value: Value) = {
    if(currentInsertRow == null) {
      throw new UnsupportedOperationException("No row is currently being built!")
    }

    currentInsertRow.set(column, value)
  }

  def setString(column: ColumnLabel, value: String) = {
    if(currentInsertRow == null) {
      throw new UnsupportedOperationException("No row is currently being built!")
    }

    currentInsertRow.set(column, value)
  }

  def setInt(column: ColumnLabel, value: Int) = {
    if(currentInsertRow == null) {
      throw new UnsupportedOperationException("No row is currently being built!")
    }

    currentInsertRow.set(column, value)
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
