package edu.berkeley.velox.datamodel

import java.util.Arrays

object PrimaryKey {
  def pkey(columns: Int*): PrimaryKey = {
    val ret = new PrimaryKey()
    ret.keyColumns = columns.toArray
    ret
  }

  def pkeyWithTable(table: Int, columns: Int*): PrimaryKey = {
    val ret = new PrimaryKey()
    ret.table = table
    ret.keyColumns = columns.toArray
    ret
  }
}

class PrimaryKey extends Comparable[PrimaryKey] {
  def this(table: Int, keyColumns: Array[Int]) {
    this()
    this.table = table
    this.keyColumns = keyColumns
  }

  def this(table: Int, columns: Int*) {
    this(table, columns.toArray)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case k: PrimaryKey => {
        table == k.table && Arrays.equals(keyColumns, k.keyColumns)
      }
      case _ => false
    }
  }

  override def hashCode: Int = {
    table*Arrays.hashCode(keyColumns)
  }

  override def toString: String = {
   table+"-"+Arrays.toString(keyColumns)
  }

  def table(table: Int): PrimaryKey = {
    this.table = table
    this
  }

  override def compareTo(other: PrimaryKey): Int = {
    if(this.equals(other))
      return 0

    val tableCompare = this.table.compareTo(other.table)
    if(tableCompare != 0) {
      return tableCompare
    }

    val hashCompare = this.hashCode.compareTo(other.hashCode)
    if(hashCompare != 0) {
      return hashCompare
    }

    val columnSizeCompare = this.keyColumns.size.compareTo(other.keyColumns.size)
    if(columnSizeCompare != 0) {
      return columnSizeCompare
    }

    var columnIndex = 0
    while(columnIndex != keyColumns.size) {
      val columnCompare = keyColumns(columnIndex).compareTo(other.keyColumns(columnIndex))
      if(columnCompare != 0) {
        return columnCompare
      }

      columnIndex += 1
    }

    return -1
  }

  var table: Int = -1
  var keyColumns: Array[Int] = null
}

