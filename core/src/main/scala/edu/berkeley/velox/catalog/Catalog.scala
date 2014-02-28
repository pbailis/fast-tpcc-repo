package edu.berkeley.velox.catalog

import edu.berkeley.velox.server.ZKClient
import edu.berkeley.velox.storage.StorageManager
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.datamodel.PrimaryKey
import edu.berkeley.velox.datamodel.Schema
import scala.collection.immutable.HashMap

object Catalog extends Logging {

  @volatile
  private var schemas = new HashMap[DatabaseName, HashMap[TableName, Schema]]

  @volatile
  private var storageManagers: List[StorageManager] = Nil

  /**
    *  Register a storage manager with the catalog.  After registration the
    *  storage manager will be informed of all relevant updates to the Catalog.
    *
    *  @param manager The storage manager to register
    *  @param notifyExisting if true, the manager will be notified of currently
    *                        existing objects in the catalog
    */
  def registerStorageManager(manager: StorageManager, notifyExisting: Boolean = false) {
    storageManagers.synchronized {
      storageManagers ::= manager
    }
    if (notifyExisting) {
      schemas.synchronized {
        schemas.foreach {
          case (dbname,tables) => {
            manager.createDatabase(dbname)
            tables.foreach {
              case (tablename,schema) => manager.createTable(dbname,tablename)
            }
          }
        }
      }
    }
  }

  def listLocalDatabases: Set[DatabaseName] = {
    schemas.keySet
  }

  def createDatabase(dbName: DatabaseName): Boolean = {
    logger.info(s"Creating database $dbName")
    ZKClient.createDatabase(dbName)
  }

  def createTable(dbName: DatabaseName, tableName: TableName, schema: Schema): Boolean = {
    logger.info(s"Creating table $dbName:$tableName")
    ZKClient.createTable(dbName, tableName, schema)
  }

  /**
   * Create a new database instance
   * @param db The name of the database
   * @return true if the database was created, false if it already existed
   */
  def _createDatabaseTrigger(db: DatabaseName): Boolean = {
    if (schemas.contains(db)) false
    else {
      schemas.synchronized {
        schemas += ((db, new HashMap[TableName, Schema]))
      }
      storageManagers foreach (_.createDatabase(db))
      true
    }
  }

  def listLocalTables(db: DatabaseName): Set[TableName] = {
    schemas.get(db) match {
      case Some(tm) => tm.keySet
      case None => null
    }
  }

  def _createTableTrigger(db: DatabaseName, table: TableName, schema: Schema): Unit = {
    val tm = schemas.getOrElse(db,throw new IllegalStateException(s"Trying to add table $table to database $db which doesn't exist"))
    if (tm.contains(table)) {
      val existing = tm(table)
      if (!schema.equals(existing))
        throw new IllegalStateException(s"Trying to modify schema for $table. Not supported!")
      else
        logger.warn(s"Duplicate call to _createTableTrigger for $db->$table with same schema.  Ignoring")
    }
    else {
      schemas.synchronized {
        // recheck for race condition
        if (tm.contains(table)) {
          val existing = tm(table)
          if (!schema.equals(existing))
            throw new IllegalStateException(s"Trying to modify schema for $table. Not supported!")
          else
            logger.warn(s"Duplicate call to _createTableTrigger for $db->$table with same schema.  Ignoring")
        } else {
          val newMap = tm + ((table,schema))
          schemas += ((db,newMap))
        }
      }
      storageManagers foreach (_.createTable(db,table))
    }
  }

  def checkDBExistsLocal(db: DatabaseName): Boolean = {
    schemas.contains(db)
  }

  /*
   * Potentially blocking call to check zookeeper
   */
  def checkTableExists(db: DatabaseName, table: TableName): Boolean = {
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
    } else if (ZKClient.checkTableExistsZookeeper(db, table)) {
      // add to local schema if table exists in zookeeper but not locally
      val schema = ZKClient.getSchemaFor(db,table)
      _createTableTrigger(db,table,schema)
      true
    } else {
      // no record of table, return false
      false
    }
  }

  def checkTableExistsLocal(db: String, table: String): Boolean = {
    if (schemas.contains(db))
      schemas.get(db).get.contains(table)
    else
      false
  }


  def extractPrimaryKey(database: DatabaseName, table: TableName, row: Row): PrimaryKey  = {
    val definition = schemas.get(database).get(table).columns.filter(_.isPrimary)
    val ret = new Array[Value](definition.size)
    var i = 0
    definition.foreach (
      col => {
        assert(row.get(col) != null)
        ret(i) = row.get(col)
        i += 1
      }
    )

    new PrimaryKey(ret)
  }
}
