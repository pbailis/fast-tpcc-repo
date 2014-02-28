package edu.berkeley.velox.storage

import java.util.concurrent.ConcurrentHashMap
import edu.berkeley.velox._
import edu.berkeley.velox.datamodel._
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedList}
import edu.berkeley.velox.catalog.Catalog
import com.typesafe.scalalogging.slf4j.Logging

class StorageManager extends Logging {

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

  def insert(databaseName: DatabaseName, tableName: TableName, insertSet: InsertSet) {
    val table =  dbs.get(databaseName).get(tableName)
    insertSet.getRows foreach {
      r => table.insert(Catalog.extractPrimaryKey(databaseName, tableName, r), r)
    }
  }

  private def isPrimaryKeyQuery(databaseName: DatabaseName,
                                tableName: TableName,
                                query: Query): Boolean = {
    query.predicate match {
      case Some(p) => {
        p match {
          case eqp: EqualityPredicate =>
            Catalog.isPrimaryKeyFor(databaseName,
                                    tableName,
                                    eqp.column)
          case _ => false
        }
      }
      case None => false
    }
  }

  def query(databaseName: DatabaseName,
            tableName: TableName,
            query: Query) : ResultSet = {
    val rows = new ArrayBuffer[Row]
    val table = dbs.get(databaseName).get(tableName)

    if (isPrimaryKeyQuery(databaseName,tableName,query)) {
      val pkv = PrimaryKey(Seq(query.predicate.get.asInstanceOf[EqualityPredicate].value))
      val row = table.get(pkv)
      if (row != null)
        rows += row.project(query.columns)
    } else {
      table.rows.values.asScala foreach {
        row => query.predicate match {
          case Some(p) => {
            p match {
              case eqp: EqualityPredicate => if(row.get(eqp.column) == eqp.value) rows += row.project(query.columns)
            }
          }
          case None => rows :+ row.project(query.columns)
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
