package edu.berkeley.velox.operations.commands

import edu.berkeley.velox.frontend.VeloxConnection
import edu.berkeley.velox.datamodel._
import scala.concurrent.Future
import edu.berkeley.velox.operations.CommandExecutor

object QueryDatabase {
  def database(dbName: String)(implicit executor: CommandExecutor): QueryDatabase = {
    new QueryDatabase(executor, dbName)
  }
}

class QueryDatabase(val executor: CommandExecutor, val name: DatabaseName) {
  def createTable(name: String, schema: Schema) : Future[QueryTable] = {
    executor.createTable(this, name, schema)
  }

  def table(name: String) : QueryTable = {
    // TODO: check if exists?
    new QueryTable(this, name)
  }

  def execute(table: QueryTable, operation: Operation) : Future[ResultSet] = {
    executor.execute(this, table, operation)
  }

  def executeBlocking(table: QueryTable, operation: Operation) : ResultSet = {
    executor.executeBlocking(this, table, operation)
  }

  def registerTrigger(tableName: TableName, triggerClass: Class[_])(implicit executor: CommandExecutor) {
    executor.registerTrigger(name, tableName, triggerClass)
  }

  def prepareQuery(table: QueryTable, queryOperation: QueryOperation): Query = {
    new Query(name, table.name, queryOperation.columns, queryOperation.predicates)
  }

  override def toString(): String = name
}
