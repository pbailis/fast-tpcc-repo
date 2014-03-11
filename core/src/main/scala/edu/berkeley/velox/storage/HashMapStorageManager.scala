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

  private def isPrimaryKeyQuery(query: Query): Boolean = {
    query.predicate match {
      case Some(p) => {
        p match {
          case eqp: EqualityPredicate =>
            Catalog.isPrimaryKeyFor(query.databaseName,
                                    query.tableName,
                                    Seq(eqp.columnIndex))
          case _ => false
        }
      }
      case None => false
    }
  }

  protected def _query(query: Query) : ResultSet = {
    val rows = new ArrayBuffer[Row]
    val table = dbs.get(query.databaseName).get(query.tableName)

    if (isPrimaryKeyQuery(query)) {
      val pkv = PrimaryKey(Array[Value](query.predicate.get.asInstanceOf[EqualityPredicate].value))
      val row = table.get(pkv)
      if (row != null)
        rows += row.project(query.columnIndexes)
    } else {
      table.rows.values.asScala foreach {
        row => query.predicate match {
          case Some(p) => {
            p match {
              case eqp: EqualityPredicate => if(row.get(eqp.columnIndex) == eqp.value) rows += row.project(query.columnIndexes)
            }
          }
          case None => rows :+ row.project(query.columnIndexes)
        }
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
