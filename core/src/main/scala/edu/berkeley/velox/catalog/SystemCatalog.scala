package edu.berkeley.velox.catalog

import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.datamodel.Schema
import java.util.concurrent.ConcurrentHashMap

class SystemCatalog {
  val schemas = new ConcurrentHashMap[DatabaseName, ConcurrentHashMap[TableName, Schema]]

  def createDatabase(name: DatabaseName) = {
    schemas.putIfAbsent(name, new ConcurrentHashMap[TableName, Schema])
  }

  def createTable(database: DatabaseName, table: TableName, schema: Schema) = {
    // TODO: not found
    schemas.get(database).putIfAbsent(table, schema)
  }

  def extractPrimaryKey(row: Row)(implicit database: DatabaseName, table: TableName)  = {
    val ret = new PrimaryKey
    schemas.get(database).get(table).pkey.value.foreach (
      col => ret.column(col, row.column(col))
    )
    ret
  }
}
