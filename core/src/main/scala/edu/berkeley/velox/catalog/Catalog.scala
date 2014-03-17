package edu.berkeley.velox.catalog

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.frontend.ClientZookeeperConnection
import edu.berkeley.velox.server.ServerZookeeperConnection
import edu.berkeley.velox.server.ZookeeperConnection
import edu.berkeley.velox.storage.StorageManager
import scala.Some
import scala.collection.immutable.HashMap

object Catalog extends Logging {

  @volatile
  protected var schemas = new HashMap[DatabaseName, HashMap[TableName, Schema]]

  @volatile
  private var storageManagers: List[StorageManager] = Nil

  var zkCon: ZookeeperConnection = null

  def initCatalog(isClient: Boolean) {
    if (isClient)
      zkCon = ClientZookeeperConnection
    else
      zkCon = ServerZookeeperConnection
  }

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

  def getSchema(db: DatabaseName, table: TableName): Schema = {
    schemas.get(db) match {
      case Some(tbls) => tbls.getOrElse(table,null)
      case None => null
    }
  }

  def isPrimaryKeyFor(db: DatabaseName, table: TableName, columns: Seq[Int]): Boolean = {
    val schema = schemas(db)(table)
    if (columns.size == schema.numPkCols) {
      // check that I've specified only pk columns
      // and for now, that they are in order
      // TODO: suport reorder in predicate
      var i = 0
      while (i < columns.length) {
        if (i != columns(i)) return false
        i+=1
      }
      true
    }
    else false
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
    } else if (zkCon.checkTableExistsZookeeper(db, table)) {
      // add to local schema if table exists in zookeeper but not locally
      val schema = zkCon.getSchemaFor(db,table)
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
    val ret = row.values.take(schemas(database)(table).numPkCols)
    new PrimaryKey(ret)
  }

  def initializeSchemaFromZK() = {
    val dbs = zkCon.listZookeeperDBs()
    logger.error("Initializing schema from zookeeper")
    dbs.foreach{  db =>
      _createDatabaseTrigger(db)
      logger.error(s"created database trigger for $db, client tables are ${zkCon.listZookeeperTables(db)}")
      zkCon.listZookeeperTables(db).map {
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
      storageManagers foreach (_.createDatabase(db))
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
            return false
          }
        } else {
          val newMap = tm + ((table,schema))
          schemas += ((db,newMap))
        }
      }
      storageManagers foreach (_.createTable(db,table))
      true
    }
  }

  def createDatabase(dbName: DatabaseName): Boolean = {
    logger.info(s"Creating database $dbName")
    // This call ensures sure the DB has been added to our local schema
    _createDatabaseTrigger(dbName)
    ClientZookeeperConnection.createDatabase(dbName)
  }

  def createTable(dbName: DatabaseName, tableName: TableName, schema: Schema): Boolean = {
    logger.info(s"Creating table $dbName:$tableName")
    _createTableTrigger(dbName, tableName, schema)
    ClientZookeeperConnection.createTable(dbName, tableName, schema)

    // create all the indexes of the table.
    schema.indexes.foreach {
      case(indexName, indexColumns) => {
        createIndex(dbName, tableName, schema, indexName, indexColumns)
      }
    }
    true
  }

  // TODO: This works currently, but think about potentially re-structuring when/where indexes schemas
  //       are translated to table schemas. The "fully correct" way may be to consult the index storage
  //       engine for the table schema.
  def createIndex(dbName: DatabaseName, tableName: TableName, tableSchema: Schema, indexName: String, indexColumns: Array[TypedColumn]): Boolean = {
    logger.info(s"Creating index $dbName:$tableName.$indexName")

    // The index columns are the prefix of the primary key.
    var prKeys: Seq[TypedColumn] = indexColumns

    // add the remaining primary keys of the base table, to make up the index's full primary key.
    tableSchema.columns.filter(_.isPrimary).foreach(typedColumn => {
      val exists = prKeys.exists(_.name == typedColumn.name)
      if (!exists) {
        // Only add the primary key if it doesn't already exist.
        prKeys = prKeys :+ typedColumn
      }
    })

    val indexSchema = new Schema
    indexSchema.setColumns(prKeys)
    val fullIndexName = tableName + "." + indexName
    // Create the index. Indexes are just tables.
    createTable(dbName, fullIndexName, indexSchema)
  }

  def registerTrigger(dbName: DatabaseName, tableName: TableName, triggerName: String, triggerBytes: Array[Byte]) = {
    ClientZookeeperConnection.createTrigger(dbName, tableName, triggerName, triggerBytes)
  }
}
