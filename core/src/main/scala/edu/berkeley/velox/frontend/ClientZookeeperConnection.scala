package edu.berkeley.velox.frontend

import edu.berkeley.velox.server.{ClientDBWatcher, ServerDBWatcher, ZookeeperConnection}
import edu.berkeley.velox.server.{ZookeeperConnectionUtils => ZKUtils}
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import edu.berkeley.velox.datamodel.{Schema, TableName, DatabaseName}
import edu.berkeley.velox.conf.VeloxConfig
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener, PathChildrenCache}
import org.apache.curator.framework.CuratorFramework
import java.util.{ArrayList => JArrayList}


object ClientZookeeperConnection extends ZookeeperConnection {

//  private val groupMembershipListeners: List[PathChildrenCacheListener] = JArrayList[PathChildrenCacheListener]

  private var cacheInitialized = false
  def addGroupMembershipListener(listener: PathChildrenCacheListener) {
//    require(cacheInitialized)
    groupMembershipCache.getListenable.addListener(listener)
  }

  // Start watching for changes in the catalog
  client.getChildren().usingWatcher(new ClientDBWatcher(client)).forPath(ZKUtils.CATALOG_ROOT)

  /**
   * The server zookeeper client cache gets registered when the server registers itself
   * with zookeeper. We need some way to trigger the initialization at the correct time on
   * the client side, hence this method. It get's called after the ClientRPCService has had
   * a chance to register it's listeners, so that those callbacks get triggered on
   * cache initialization.
   */
  def initializeClientGroupMembershipCache() {
    // TODO separate out cache creation and start
    initializeCache(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT)
    cacheInitialized = true
  }

  /**
   * This lock ensures that there is only one
   * schema change going on at a time. Server's must acquire this
   * lock before attempting to initiate a schema change.
   */
  val schemaChangeLock = new InterProcessMutex(client, ZKUtils.SCHEMA_LOCK_PATH)

  def createDatabase(dbName: DatabaseName): Boolean = {
    addToSchema(dbName, null, null)
  }

  def createTable(dbName: DatabaseName, tableName: TableName, schema: Schema): Boolean = {
    addToSchema(dbName, tableName, schema)
  }


  private def addToSchema(dbName: DatabaseName, tableName: TableName, schema: Schema): Boolean = {
    var ret = true
    try {
      // make sure only one schema change at a time because all schema changes use
      // the same barrier
      logger.debug("Acquiring lock")
      schemaChangeLock.acquire()
      logger.debug("lock acquired")

      // reset the counter
      schemaChangeBarrier.reset(VeloxConfig.expectedNumInternalServers)

      if (tableName != null) {
        client.create().forPath(ZKUtils.makeTablePath(dbName, tableName), ZKUtils.schemaToBytes(schema))
      } else {
        client.create().forPath(ZKUtils.makeDBPath(dbName))
      }
      // update catalog - triggers watchers who will actually add
      // DB to local schema when they detect the change

      // wait until all servers have made change
      schemaChangeBarrier.awaitUntilZero()
    }
    catch {
      // TODO better error handling
      case e: Exception => {
        logger.error("Error adding to schema", e)
        ret = false
      }
    } finally {
      schemaChangeLock.release()
    }
    logger.debug("finishing schema addition")
    ret
  }
}
