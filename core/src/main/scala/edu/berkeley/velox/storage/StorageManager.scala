package edu.berkeley.velox.storage

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.trigger.TriggerManager
import edu.berkeley.velox.catalog.Catalog
import edu.berkeley.velox.trigger.server._
import edu.berkeley.velox.operations.CommandExecutor
import scala.concurrent.Future
import edu.berkeley.velox.operations.commands.{QueryOperation, InsertionOperation, Operation, QueryDatabase, QueryTable}
import edu.berkeley.velox.util.NonThreadedExecutionContext.context

trait StorageManager extends CommandExecutor {

  /**
    * Create a new database. No-op if db already exists.
    *
    * @param dbName Name of the database to create
    */
  def createDatabaseLocal(dbName: DatabaseName)

  /**
    *  Create a table within a database.  No-op if table already exists
    *
    *  @param dbName Database to create table in
    *  @param tableName Name of Table to create
    */
  final def createTableLocal(dbName: DatabaseName, tableName: TableName) {
    _createTableLocal(dbName, tableName)
    val schema = Catalog.getSchema(dbName, tableName)

    // If table has indexes, add trigger to update indexes.
    // TODO: support alternative index update methods (specified by table schema).

    // Assuming '.' is delimiter "tableName.indexName".
    val parts = tableName.split("\\.")
    if (parts.length > 1) {
      // This "table" is an index for a table.
      // Add (or re-add) a trigger for the table.
      val baseTable = parts(0)
      val tableSchema = Catalog.getSchema(dbName, baseTable)
      val trigger = new IndexUpdateTrigger(dbName, baseTable)
      val triggerName = trigger.getClass.getName
      TriggerManager.addTriggerInstance(dbName, baseTable, triggerName, trigger)
    } else {
      // This "table" is a table.
      // If table schema contains indexes, add (or re-add) a trigger for the table.
      if (!schema.indexes.isEmpty) {
        val trigger = new IndexUpdateTrigger(dbName, tableName)
        val triggerName = trigger.getClass.getName
        TriggerManager.addTriggerInstance(dbName, tableName, triggerName, trigger)
      }
    }
  }

  /**
    * Insert a set of values into a table.
    *
    * @param databaseName Database to insert into
    * @param tableName Table to insert into
    * @param insertSet Set of values to insert
    *
    * @return The number of rows inserted
    */
  final def insertLocal(databaseName: DatabaseName, tableName: TableName, insertSet: InsertSet): Int = {
    _insertLocal(databaseName, tableName, insertSet)
  }

  /**
    * Run a query against a table.
    *
    * @param query Query to run
    *
    * @return Set of values that answer the query
    */
  final def query(query: Query): ResultSet = {
    _queryLocal(query)
  }

  def execute(database: QueryDatabase, table: QueryTable, operation: Operation) : Future[ResultSet] = {
    Future{
      executeBlocking(database, table, operation)
    }
  }

  def executeBlocking(database: QueryDatabase, table: QueryTable, operation: Operation): ResultSet = {
    operation match {
      case s: QueryOperation => {
        _queryLocal(new Query(database.name, table.name, s.columns, s.predicates))
      }
      case i: InsertionOperation => {
        _insertLocal(database.name, table.name, i.insertSet); new ResultSet
      }
    }
  }

  /*
   * Implementors of storage engines should implement the following methods.
   */

  protected def _createTableLocal(dbName: DatabaseName, tableName: TableName)
  protected def _insertLocal(databaseName: DatabaseName, tableName: TableName, insertSet: InsertSet): Int
  protected def _queryLocal(query: Query): ResultSet
}
