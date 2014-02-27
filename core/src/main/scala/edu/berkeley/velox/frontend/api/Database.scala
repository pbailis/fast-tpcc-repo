package edu.berkeley.velox.frontend.api

import edu.berkeley.velox.datamodel.api.operation.Operation
import edu.berkeley.velox.frontend.VeloxConnection
import edu.berkeley.velox.datamodel.{ResultSet, DatabaseName, TableName, Schema}
import scala.concurrent.Future

class Database(val connection: VeloxConnection, val name: DatabaseName) {
  def createTable(name: String, schema: Schema) : Future[Table] = {
    connection.createTable(this, name, schema)
  }

  def table(name: String) : Table = {
    // TODO: check if exists?
    new Table(this, name)
  }

  def execute(table: Table, operation: Operation) : Future[ResultSet] = {
    connection.execute(this, table, operation)
  }

  def registerTrigger(tableName: TableName, triggerClass: Class[_]) {
    connection.registerTrigger(name, tableName, triggerClass)
  }
}
