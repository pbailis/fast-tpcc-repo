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

  def extractPrimaryKey(database: DatabaseName, table: TableName, row: Row): PrimaryKey  = {
    val definition = schemas.get(database).get(table).pkey
    val ret = new Array[Value](definition.value.size)
    var i = 0
    definition.value.foreach (
      col => {
        ret(i) = row.get(col)
        i += 1
      }
    )
    new PrimaryKey(ret)
  }
}
