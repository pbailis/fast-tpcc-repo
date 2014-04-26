package edu.berkeley.velox.operations

import edu.berkeley.velox.operations.commands._
import scala.concurrent._
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.util.JVMClassClosureUtil
import edu.berkeley.velox.catalog.Catalog
import edu.berkeley.velox.util.NonThreadedExecutionContext.context
import edu.berkeley.velox.udf._
import edu.berkeley.velox.datamodel.ColumnLabel
import edu.berkeley.velox.operations.internal.PerPartitionUDFRequest
import edu.berkeley.velox.datamodel.ColumnLabel

private[velox] trait CommandExecutor {
  def execute(database: QueryDatabase, table: QueryTable, operation: Operation) : Future[ResultSet]
  def executeBlocking(database: QueryDatabase, table: QueryTable, operation: Operation) : ResultSet

  def insert(values: (ColumnLabel, Value)*) : InsertionOperation = {
    new InsertionOperation(values)
  }

  // Query creation/parsing/validation
  def select(names: ColumnLabel*) : QueryOperation = {
    new QueryOperation(null, names)
  }

  def database(name: DatabaseName) : QueryDatabase = {
    new QueryDatabase(this, name)
  }

  def createDatabase(name: DatabaseName) : Future[QueryDatabase] = {
    future {
      Catalog.createDatabase(name)
      new QueryDatabase(this, name)
    }
  }

  def createTable(database: QueryDatabase, tableName: TableName, schema: Schema) : Future[QueryTable] = {
    future {
      Catalog.createTable(database.name, tableName, schema)
      new QueryTable(database, tableName)
    }
  }

  // blocking command to register a new trigger.
  def registerTrigger(dbName: DatabaseName, tableName: TableName, triggerClass: Class[_]) {
    val className = triggerClass.getName
    val classBytes = JVMClassClosureUtil.classToBytes(triggerClass)
    Catalog.registerTrigger(dbName, tableName, className, classBytes)
  }
}