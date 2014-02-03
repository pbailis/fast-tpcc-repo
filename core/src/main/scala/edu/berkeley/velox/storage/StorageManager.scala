package edu.berkeley.velox.storage

import java.util.concurrent.ConcurrentHashMap
import edu.berkeley.velox._
import edu.berkeley.velox.datamodel._
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedList}
import edu.berkeley.velox.catalog.SystemCatalog
import edu.berkeley.velox.datamodel.Column

/**
 * Created by crankshaw on 2/6/14.
 * This is the beginning of a backend storage API.
 */
class StorageManager(val catalog: SystemCatalog) {

  // Correct but dumb implementations of database and tables.
  // No attempt to be fast or efficient, no schema for now,
  // no error handling.
  val dbs = new ConcurrentHashMap[DatabaseName, ConcurrentHashMap[TableName, Table]]()

  /**
   * Create a new database. Nop if db already exists.
   * @param name
   */
  def createDatabase(name: DatabaseName) {
    dbs.putIfAbsent(name, new ConcurrentHashMap[TableName, Table]())
  }

  def createTable(dbName: String, tableName: String) {
    if (dbs.contains(dbName)) {
      dbs.get(dbName).putIfAbsent(tableName, new Table)
    }
  }

  def checkTableExistence(dbName: DatabaseName, tableName: TableName): Boolean  = {
    dbs.containsKey(dbName) && dbs.get(dbName).containsKey(tableName)
  }

  def checkDBExistence(dbName: DatabaseName): Boolean = {
    dbs.containsKey(dbName)
  }

  def getDBNames = {
    dbs.keySet().asScala
  }

  def insert(database: DatabaseName, table: TableName, row: Row) {
    dbs.get(database).get(table).insert(catalog.extractPrimaryKey(row), row)
  }

  def select(database: DatabaseName,
             table: TableName,
             columns: Seq[Column],
             predicate: Option[Predicate] = None) : ResultSet = {
    val rows = new ArrayBuffer[Row]

    val table = dbs.get(database).get(table)

    table.rows.values.asScala foreach {
      row => predicate match {
        case Some(p) => {
          p match {
            case eqp: EqualityPredicate => if(row.column(eqp.column) == eqp.value) rows += row.project(columns)
          }
        }
        case None => rows :+ row.project(columns)
      }
    }


    new ResultSet(rows)
  }
}

class Table {
  // question: why can't we make this a PrimaryKey, Row?
  val rows = new ConcurrentHashMap[Row, Row]()

  def get(key: Row) : Row = {
    rows.get(key)
  }

  def insert(key: Row, row: Row) {
    rows.put(key, row)
  }
}

