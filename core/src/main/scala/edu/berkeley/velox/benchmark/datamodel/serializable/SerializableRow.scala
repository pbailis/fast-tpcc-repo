package edu.berkeley.velox.benchmark.datamodel.serializable

import java.util
import edu.berkeley.velox.datamodel.Row
import com.typesafe.scalalogging.slf4j.Logging

object SerializableRowGenerator {
  val NULL = new SerializableRow

  def columnForUpdate(column: Int): SerializableRow = {
    val ret = new SerializableRow
    ret.columnForUpdate(column)
    return ret
  }

  def fetchColumn(column: Int): SerializableRow = {
    val ret = new SerializableRow
    ret.fetchColumn(column)
    return ret
  }

  def column(column: Int, value: Any): SerializableRow = {
    val ret = new SerializableRow
    ret.column(column, value)
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
    this.column(column)
    this
  }
}
