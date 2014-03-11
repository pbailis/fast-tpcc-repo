package edu.berkeley.velox.catalog

import edu.berkeley.velox.storage.StorageManager
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.datamodel.Schema

object ServerCatalog extends Catalog {

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


  override def _createDatabaseTrigger(db: DatabaseName): Boolean = {
    val added = super._createDatabaseTrigger(db)
    if (added) {
      storageManagers foreach (_.createDatabase(db))
    }
    added
  }


  override def _createTableTrigger(db: DatabaseName, table: TableName, schema: Schema): Boolean = {
    val added = super._createTableTrigger(db, table, schema)
    if (added) {
      storageManagers foreach (_.createTable(db,table))
    }
    added
  }
}
