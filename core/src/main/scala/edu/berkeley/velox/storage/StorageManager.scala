package edu.berkeley.velox.storage

import java.util.concurrent.ConcurrentHashMap
import edu.berkeley.velox._
import edu.berkeley.velox.datamodel._
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedList}
import edu.berkeley.velox.catalog.Catalog
import com.typesafe.scalalogging.slf4j.Logging

trait StorageManager {

  Catalog.registerStorageManager(this,true)

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
  def createTable(dbName: DatabaseName, tableName: TableName)

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
  final def query(databaseName: DatabaseName,  tableName: TableName, query: Query): ResultSet = {
    _query(databaseName, tableName, query)
  }

  /*
   * Implementors of storage engines should implement the following methods to insert and query data
   */

  protected def _insert(databaseName: DatabaseName, tableName: TableName, insertSet: InsertSet): Int
  protected def _query(databaseName: DatabaseName, tableName: TableName, query: Query): ResultSet
}
