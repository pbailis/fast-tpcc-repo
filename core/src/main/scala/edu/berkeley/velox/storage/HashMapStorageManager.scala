package edu.berkeley.velox.storage

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox._
import edu.berkeley.velox.catalog.Catalog
import edu.berkeley.velox.datamodel._
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class HashMapStorageManager extends StorageManager with Logging {

  // Correct but dumb implementations of database and tables.
  // No attempt to be fast or efficient, no schema for now,
  // no error handling.
  val dbs = new ConcurrentHashMap[DatabaseName, ConcurrentHashMap[TableName, Table]]()

  Catalog.registerStorageManager(this,true)

  /**
   * Create a new database. Nop if db already exists.
   * @param name
   */
  def createDatabase(name: DatabaseName) {
    dbs.putIfAbsent(name, new ConcurrentHashMap[TableName, Table]())
  }

  def createTable(dbName: DatabaseName, tableName: TableName) {
    if (dbs.containsKey(dbName)) {
      dbs.get(dbName).putIfAbsent(tableName, new Table)
    } else {
      throw new IllegalStateException(s"Table $tableName creation request, but parent database $dbName not found! $dbs")

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

  protected def _insert(databaseName: DatabaseName, tableName: TableName, insertSet: InsertSet): Int = {
    val table =  dbs.get(databaseName).get(tableName)
    var i = 0
    insertSet.getRows foreach {
      r => {
        table.insert(Catalog.extractPrimaryKey(databaseName, tableName, r), r)
        i+=1
      }
    }
    i
  }

  // Find and return a predicate that can be used as a key to
  // look up the result, or null if there isn't one.
  private def pkFromQuery(query: Query): PrimaryKey = {
    query.predicates.foreach(pred =>
      pred match {
        case eqp: EqualityPredicate => {
          if ( Catalog.isPrimaryKeyFor(query.databaseName,
                                       query.tableName,
                                       Seq(eqp.columnIndex)) ) {
            return PrimaryKey(Array[Value](eqp.value))
          }
        }
      })
    null
  }

  protected def _query(query: Query) : ResultSet = {
    val rows = new ArrayBuffer[Row]
    val table = dbs.get(query.databaseName).get(query.tableName)

    val key = pkFromQuery(query)

    if (key != null) {
      val row = table.get(key)
      if ( (row != null) &&
           ((query.predicates.size <= 1 || row.matches(query.predicates))) )
        rows += row.project(query.columnIndexes)
    } else {
      val it = table.rows.values.iterator
      val needMatch = query.predicates.size > 0
      while (it.hasNext) {
        val row = it.next
        if (!needMatch || row.matches(query.predicates))
          rows :+ row.project(query.columnIndexes)
      }
    }

    new ResultSet(rows)
  }
}

class Table {
  val rows = new ConcurrentHashMap[PrimaryKey, Row]()

  def get(key: PrimaryKey) : Row = {
    rows.get(key)
  }

  def insert(key: PrimaryKey, row: Row) {
    rows.put(key, row)
  }
}
