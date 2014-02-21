package edu.berkeley.velox.storage

import java.util.concurrent.ConcurrentHashMap
import edu.berkeley.velox._
import edu.berkeley.velox.datamodel.{Value, Key}
import scala.collection.JavaConverters._

/**
 * Created by crankshaw on 2/6/14.
 * This is the beginning of a backend storage API.
 */
class StorageManager {

  // Correct but dumb implementations of tables.
  // No attempt to be fast or efficient, no schema for now,
  // no error handling.
  val tables = new ConcurrentHashMap[String, TableInstance]()
  //val dbs = new ConcurrentHashMap[String, Boolean]()

  def addTable(name: String) {
      tables.putIfAbsent(name, new TableInstance)
  }

  def put(tableName: String, dbName: String, k: Key, v: Value): Value = {
    tables.get(StorageManager.qualifyTableName(dbName, tableName)).put(k, v)
  }

  def get(tableName: String, dbName: String, k: Key): Value = {
    tables.get(StorageManager.qualifyTableName(dbName, tableName)).get(k)
  }

  def insert(tableName: String, dbName: String, k: Key, v: Value): Boolean = {
    tables.get(StorageManager.qualifyTableName(dbName, tableName)).put(k, v) != null
  }

}

object StorageManager {
  def qualifyTableName(db: String, table: String): String = {
    s"$db:$table"
  }

}
