package edu.berkeley.velox.benchmark

import java.util.Arrays
import scala.collection.JavaConversions._
import edu.berkeley.velox.datamodel.ItemKey

object TPCCItemKey {
  def key(table_id: Int, columns: Int*): TPCCItemKey = {
    return new TPCCItemKey(table_id, columns.toArray)
  }
}

class TPCCItemKey extends ItemKey {
  def this(table_id: Int, columns: Array[Int]) {
    this()
    this.table_id = table_id
    this.columns = columns
  }

  def this(table_id: Int, columns: Int*) {
    this(table_id, columns.toArray)
  }


  def w_id: Int = {
    return columns(0)
  }

  def table: Int = {
    return table_id
  }

  override def equals(other: Any): Boolean = {
    other match {
      case k: TPCCItemKey => {
        table_id == k.table_id && Arrays.equals(columns, k.columns)
      }
      case _ => false
    }
  }

  override def hashCode: Int = {
    table_id + Arrays.hashCode(columns)
  }

  override def toString: String = {
   table_id+"-"+Arrays.toString(columns)
  }

  private var table_id: Int = 0
  private var columns: Array[Int] = null
}

