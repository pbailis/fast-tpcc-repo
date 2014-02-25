package edu.berkeley.velox.benchmark.datamodel

import edu.berkeley.velox.datamodel.{PrimaryKey, Row}
import com.typesafe.scalalogging.slf4j.Logging

class Table extends Logging {
  def this(tableName: Int, parentTxn: Transaction) {
    this()
    this.tableName = tableName
    this.parentTxn = parentTxn
  }

  def getName: Int = {
    return tableName
  }

  def put(pkey: PrimaryKey, row: Row): Table = {
    pkey.table = tableName
    parentTxn.put(pkey, row)
    return this
  }

  def get(pkey: PrimaryKey, row: Row): Table = {
    pkey.table = tableName

    parentTxn.get(pkey, row)
    return this
  }

  private var tableName: Int = 0
  private var parentTxn: Transaction = null
}

