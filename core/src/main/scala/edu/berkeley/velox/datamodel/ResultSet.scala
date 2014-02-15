package edu.berkeley.velox.datamodel

import java.util.List
import scala.collection.JavaConverters._

class ResultSet {
  private var position: Int = 0
  private var rows: Seq[Row] = null

  protected[velox] def this(rows: Seq[Row]) {
    this()
    this.rows = rows
  }

  def absolute(resultNo: Int) {
    if(rows == null) {
      throw new UnsupportedOperationException(s"No rows found in set!")
    } else if (resultNo > rows.size-1) {
      throw new UnsupportedOperationException(s"Position $resultNo requested, but maximum position is ${rows.size-1}")
    }

    this.position = position
  }

  def next() {
    if(rows == null) {
      throw new UnsupportedOperationException(s"No rows found in set!")
    } else if(position+1 > rows.size) {
      throw new UnsupportedOperationException(s"Next requested, but maximum position is ${rows.size-1}")
    }

    position += 1
  }

  def prev() {
    if(rows == null) {
      throw new UnsupportedOperationException(s"No rows found in set!")
    } else if(position == 0) {
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

    position = rows.length-1
  }

  def getInt(columnName: ColumnLabel): Int = {
    rows(position).get(columnName).asInt
  }

  def getString(columnName: ColumnLabel): String = {
    rows(position).get(columnName).asString
  }

  def getColumnLabels(): Seq[ColumnLabel] = {
    rows(position).getColumnLabels
  }

  // TODO: do we want to support this operation?
  def size(): Int = {
    rows.length
  }

  // TODO: is this copy going to be too expensive?
  def merge(other: ResultSet) {
    rows ++ other.rows
  }
}
