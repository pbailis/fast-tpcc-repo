package edu.berkeley.velox.datamodel

import java.util.List
import scala.collection.JavaConverters._

// N.B.: in the future, we should consider turning this into a trait
class ResultSet {
  // Default to point before the first row.
  private var position: Int = -1
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

  // Moves the cursor forward one row from its current position.
  // Returns true if the current row is valid, false when it points to after the last row.
  def next(): Boolean = {
    if(rows == null) {
      throw new UnsupportedOperationException(s"No rows found in set!")
    } else if(position+1 > rows.size) {
      throw new UnsupportedOperationException(s"Next requested, but maximum position is ${rows.size-1}")
    }

    position += 1
    position < rows.size
  }

  def hasNext(): Boolean = {
    position < rows.size-1
  }

  def prev() {
    if(rows == null) {
      throw new UnsupportedOperationException(s"No rows found in set!")
    } else if(position == 0) {
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

  def end() {
    if(rows == null) {
      throw new UnsupportedOperationException(s"No rows found in set!")
    }

    position = rows.length-1
  }

  def getInt(at: Int): Int = {
    rows(position).get(at).asInt
  }

  def getString(at: Int): String = {
    rows(position).get(at).asString
  }

  //def getColumnLabels(): Seq[ColumnLabel] = {
  //  rows(position).getColumnLabels
  //}

  // TODO: do we want to support this operation?
  def size(): Int = {
    rows.length
  }

  // TODO: is this copy going to be too expensive?
  def merge(other: ResultSet) {
    if(rows == null) {
      rows = other.rows
    }

    if(other.rows != null) {
      rows ++ other.rows
    }
  }
}
