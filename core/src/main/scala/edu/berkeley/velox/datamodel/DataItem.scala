package edu.berkeley.velox.datamodel

import java.util.Collection
import java.util

object DataItem {
  val NULL = new DataItem
}

class DataItem {
    var timestamp = Timestamp.NO_TIMESTAMP
    var value: Any = null
    var transactionKeys: util.ArrayList[ItemKey] = null

    def this(timestamp: Long, value: Any) {
      this()
      this.timestamp = timestamp
      this.value = value
    }

  def this(timestamp: Long, value: Any, transactionKeys: util.ArrayList[ItemKey]) {
    this(timestamp, value)
    this.transactionKeys = transactionKeys
  }
}
