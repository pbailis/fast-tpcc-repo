package edu.berkeley.velox.catalog

import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.immutable.HashMap
import edu.berkeley.velox.datamodel._
import scala.Some
import scala.Some
import edu.berkeley.velox.storage.StorageManager
import scala.Some
import edu.berkeley.velox.server.ServerZookeeperConnection
import scala.Some
import scala.Some
import edu.berkeley.velox.datamodel.PrimaryKey
import scala.Some
import edu.berkeley.velox.datamodel.ColumnLabel
import edu.berkeley.velox.frontend.ClientZookeeperConnection

trait Catalog extends Logging {


  @volatile
  protected var schemas = new HashMap[DatabaseName, HashMap[TableName, Schema]]

  def getSchema(db: DatabaseName, table: TableName): Schema = {
    schemas.get(db) match {
      case Some(tbls) => tbls.getOrElse(table,null)
      case None => null
    }
  }

  def isPrimaryKeyFor(db: DatabaseName, table: TableName, column: ColumnLabel): Boolean = {
    val pk = schemas(db)(table).primaryKey
    pk.size == 1 && pk.head.equals(column)
  }

  def listLocalDatabases: Set[DatabaseName] = {
    schemas.keySet
  }


  def listLocalTables(db: DatabaseName): Set[TableName] = {
    schemas.get(db) match {
      case Some(tm) => tm.keySet
      case None => null
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
    } else if (ServerZookeeperConnection.checkTableExistsZookeeper(db, table)) {
      // add to local schema if table exists in zookeeper but not locally
      val schema = ServerZookeeperConnection.getSchemaFor(db,table)
      _createTableTrigger(db,table,schema)
      true
    } else {
      // no record of table, return false
      false
    }
  }

  def checkTableExistsLocal(db: String, table: String): Boolean = {
    if (schemas.contains(db))
      schemas(db).contains(table)
    else
      false
  }


  def extractPrimaryKey(database: DatabaseName, table: TableName, row: Row): PrimaryKey  = {
    val definition = schemas(database)(table).primaryKey
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

  def initializeSchemaFromZK() = {
    val dbs = ClientZookeeperConnection.listZookeeperDBs()
    logger.error("Initializing schema from zookeeper")
    dbs.foreach{  db =>
      _createDatabaseTrigger(db)
      logger.error(s"created database trigger for $db, client tables are ${ClientZookeeperConnection.listZookeeperTables(db)}")
      ClientZookeeperConnection.listZookeeperTables(db).map {
        case (k, v) =>
          logger.error("init schema success")
          _createTableTrigger(db, k, v)
      }
    }
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
      true
    }
  }

  def _createTableTrigger(db: DatabaseName, table: TableName, schema: Schema): Boolean = {
    val tm = schemas.getOrElse(db,throw new IllegalStateException(s"Trying to add table $table to database $db which doesn't exist"))
    if (tm.contains(table)) {
      val existing = tm(table)
      if (!schema.equals(existing)) {
        throw new IllegalStateException(s"Trying to modify schema for $table. Not supported!")
      }
      else {
        logger.warn(s"Duplicate call to _createTableTrigger for $db->$table with same schema.  Ignoring")
      }
      false
    }
    else {
      schemas.synchronized {
        // recheck for race condition
        if (tm.contains(table)) {
          val existing = tm(table)
          if (!schema.equals(existing)) {
            throw new IllegalStateException(s"Trying to modify schema for $table. Not supported!")
          }
          else {
            logger.warn(s"Duplicate call to _createTableTrigger for $db->$table with same schema.  Ignoring")
            false
          }
        } else {
          val newMap = tm + ((table,schema))
          schemas += ((db,newMap))
        }
      }
      true
    }
  }
}