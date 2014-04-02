package edu.berkeley.velox.storage

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox._
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.trigger.TriggerManager
import edu.berkeley.velox.catalog.Catalog
import edu.berkeley.velox.trigger.server._

trait StorageManager {

  /**
    * Create a new database. No-op if db already exists.
    *
    * @param dbName Name of the database to create
    */
  def createDatabase(dbName: DatabaseName)

  /**
    *  Create a table within a database.  No-op if table already exists
    *
    *  @param dbName Database to create table in
    *  @param tableName Name of Table to create
    */
  final def createTable(dbName: DatabaseName, tableName: TableName) {
    _createTable(dbName, tableName)
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
  final def insert(databaseName: DatabaseName, tableName: TableName, insertSet: InsertSet): Int = {
    _insert(databaseName, tableName, insertSet)
  }

  /**
    * Run a query against a table.
    *
    * @param databaseName Database to query
    * @param tableName Table to query
    * @param query Query to run
    *
    * @return Set of values that answer the query
    */
  final def query(query: Query): ResultSet = {
    _query(query)
  }

  /*
   * Implementors of storage engines should implement the following methods.
   */

  protected def _createTable(dbName: DatabaseName, tableName: TableName)
  protected def _insert(databaseName: DatabaseName, tableName: TableName, insertSet: InsertSet): Int
  protected def _query(query: Query): ResultSet
}
