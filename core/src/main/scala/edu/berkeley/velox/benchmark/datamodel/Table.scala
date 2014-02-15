package edu.berkeley.velox.benchmark.datamodel

import java.util.Arrays
import edu.berkeley.kaiju.storedproc.datamodel.Row
import edu.berkeley.velox.benchmark.TPCCItemKey
import scala.collection.JavaConversions._

class Table {
  def this(tableName: Int, parentTxn: Transaction) {
    this()
    this.tableName = tableName
    this.parentTxn = parentTxn
  }

  def getName: Int = {
    return tableName
  }

  def put(row: Row): Table = {
    for (column <- row.getColumns) {
      val totalNumberColumns: Int = row.numColumns
      val allColumns: Array[Int] = Arrays.copyOf(row.getKeys, totalNumberColumns)
      allColumns(totalNumberColumns - 1) = column
      parentTxn.put(new TPCCItemKey(tableName, allColumns), row.getValue(column))
    }
    return this
  }

  def get(row: Row): Table = {
    for (column <- row.getColumns) {
      val totalNumberColumns: Int = row.numColumns
      val allColumns: Array[Int] = Arrays.copyOf(row.getKeys, totalNumberColumns)
      allColumns(totalNumberColumns - 1) = column
      parentTxn.get(new TPCCItemKey(tableName, allColumns))
    }
    return this
  }

  private var tableName: Int = 0
  private var parentTxn: Transaction = null
}

