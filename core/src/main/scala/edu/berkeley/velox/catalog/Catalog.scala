package edu.berkeley.velox.catalog

import edu.berkeley.velox.server.ZKClient
import java.util.concurrent.ConcurrentHashMap
import edu.berkeley.velox.storage.StorageManager
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.datamodel.PrimaryKey
import edu.berkeley.velox.datamodel.Schema

class Catalog(storage: StorageManager) extends Logging {

  storage.setCatalog(this)

  var zkClient: ZKClient = null
  var registered = false

  def setZKClient(zk: ZKClient) {
    zkClient = zk
    registered = true
  }

  val schemas = new ConcurrentHashMap[DatabaseName, ConcurrentHashMap[DatabaseName, Schema]]

  def listLocalDatabases: Set[DatabaseName] = {
    // convert from Java to Scala set under the covers. This sucks.
    schemas.keySet().asScala.toSet
  }

  def createDatabase(dbName: DatabaseName): Boolean = {
    assume(registered)
    logger.info(s"Creating database $dbName")
    zkClient.createDatabase(dbName)
  }

  def createTable(dbName: DatabaseName, tableName: TableName, schema: Schema): Boolean = {
    assume(registered)
    logger.info(s"Creating table $dbName:$tableName")
    zkClient.createTable(dbName, tableName, schema)
  }

  /**
   * Create a new database instance
   * @param db The name of the database
   * @return true if the database was created, false if it already existed
   */
  def _createDatabaseTrigger(db: DatabaseName): Boolean = {
    val dbAdded = schemas.putIfAbsent(db.toString, new ConcurrentHashMap[TableName, Schema]) == null

    if (dbAdded) {
      storage.createDatabase(db)
    }

    return dbAdded
  }

  def listLocalTables(db: DatabaseName): Set[TableName] = {
    val tableMap = schemas.get(db)

    if(tableMap == null) {
      return null
    }

    tableMap.keySet().asScala.toSet
  }

  def _createTableTrigger(db: DatabaseName, table: TableName, schema: Schema): Boolean = {
    if (!checkDBExistsLocal(db)) {
      throw new IllegalStateException(s"Trying to add table $table to database $db which doesn't exist")
    }
    val tableAdded = schemas.get(db).putIfAbsent(table, schema) == null
    if (tableAdded) {
      storage.createTable(db, table)
    } else {
      throw new IllegalStateException(s"Trying to add table $table to database $db but table $table already exists!")
    }
    tableAdded
  }

  def checkDBExistsLocal(db: DatabaseName): Boolean = {
    schemas.containsKey(db)
  }

  /*
   * Potentially blocking call to check zookeeper
   */
  def checkTableExists(db: DatabaseName, table: TableName): Boolean = {
    require(registered)
    /* Because there can be only one schema change at time
     * and the only way a schema exists on ZK but not locally is
     * if we are in the middle of a schema change, we know that if the
     * db does not exist locally the table doesn't exist and can return false
     * immediately.
     */
    if (!checkDBExistsLocal(db)) {
      false
    } else if (checkTableExistsLocal(db, table)) {
      true
    } else if (zkClient.checkTableExistsZookeeper(db, table)) {
      // add to local schema if table exists in zookeeper but not locally
      storage.createTable(db, table)
      true
    } else {
      // no record of table, return false
      false
    }
  }

  def checkTableExistsLocal(db: String, table: String): Boolean = {
    checkDBExistsLocal(db) && schemas.get(db).contains(table)
  }


  def extractPrimaryKey(database: DatabaseName, table: TableName, row: Row): PrimaryKey  = {
    val definition = schemas.get(database).get(table).pkey
    val ret = new Array[Value](definition.value.size)
    var i = 0
    definition.value.foreach (
      col => {
        assert(row.get(col) != null)
        ret(i) = row.get(col)
        i += 1
      }
    )

    new PrimaryKey(ret)
  }
}
