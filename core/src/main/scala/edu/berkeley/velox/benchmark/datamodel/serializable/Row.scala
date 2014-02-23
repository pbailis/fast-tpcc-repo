package edu.berkeley.velox.benchmark.datamodel.serializable

import java.util
import edu.berkeley.velox.datamodel.Row

object SerializableRow extends Row {
  val NULL = new Row

  def columnForUpdate(column: Int): SerializableRow = {
    val ret = new SerializableRow
    ret.column(column)
    ret.forUpdate = true
    ret
  }

  def fetchColumn(column: Int): SerializableRow = {
    val ret = new SerializableRow
    ret.fetchColumn(column)
    ret
  }
}

class SerializableRow extends Row {
  var forUpdate = false
  var needsLock = true

  def columnForUpdate(column: Int): SerializableRow = {
    forUpdate = true
    this.fetchColumn(column)
  }

  def fetchColumn(column: Int): SerializableRow = {
    columns.put(column, null)
    this
  }
}
