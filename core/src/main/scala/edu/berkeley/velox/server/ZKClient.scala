package edu.berkeley.velox.server

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.zookeeper.{WatchedEvent, CreateMode}
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.NetworkDestinationHandle
import java.net.InetSocketAddress
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.curator.framework.api.CuratorWatcher
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.utils.ZKPaths
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.util.zk.DistributedCountdownLatch
import edu.berkeley.velox.catalog.Catalog
import edu.berkeley.velox.datamodel.{Schema, TableName, DatabaseName}
import edu.berkeley.velox.util.KryoThreadLocal
import java.nio.ByteBuffer


/**
 * This client is threadsafe. The CuratorFramework
 * and Recipes classes are threadsafe, and almost all of the mutable state is contained
 * in them. There should be one shared client per JVM.
 */
object ZKClient extends Logging {

  protected val VELOX_NAMESPACE = "velox"
  protected val ZK_UTIL_PATH = "/velox-utils"
  protected val CATALOG_ROOT = "/catalog"
  protected val SCHEMA_BARRIER_PATH = ZKPaths.makePath(ZK_UTIL_PATH, "schema-barrier")
  protected val SCHEMA_LOCK_PATH = ZKPaths.makePath(ZK_UTIL_PATH, "schema-change-lock")
  protected val CLUSTER_GROUP_NODE = ZKPaths.makePath(ZK_UTIL_PATH, "velox-cluster")

  /**
   * Strips the last 10 characters from a path and creates an Int from them.
   * This assumes Zookeeper's sequential node naming format.
   * @param path The full Zookeeper path of a sequentially named node
   * @return The sequential ID assigned this node by Zookeeper
   */
  def getServerIdFromPath(path: String): NetworkDestinationHandle = {
    path.substring(path.length - 10).toInt
  }

  def makeDBPath(dbName: String): String = {
    ZKPaths.makePath(CATALOG_ROOT, dbName)
  }

  def makeTablePath(db: String, tbl: String): String = {
    ZKPaths.makePath(makeDBPath(db), tbl)
  }

  private var groupMembershipCache = null.asInstanceOf[PathChildrenCache]
  private var registered = false
  @volatile private var serversInGroup = Map[NetworkDestinationHandle, InetSocketAddress]().empty

  private val client = CuratorFrameworkFactory.builder()
    .namespace(VELOX_NAMESPACE)
    .connectString(VeloxConfig.zookeeperServerAddresses)
    .retryPolicy(new ExponentialBackoffRetry(1000, 10))
    .build()
  client.start() // clients must be started before they can be used

  try {
    client.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .forPath(CLUSTER_GROUP_NODE)
  } catch {
    case e: NodeExistsException => logger.info("No need to create group node, already exists.")
  }

  try {
    client.create()
      .withMode(CreateMode.PERSISTENT)
      .forPath(CATALOG_ROOT)
  } catch {
    case e: NodeExistsException => logger.info("Someone else already created catalog")
  }
  // Start watching for changes in the catalog
  client.getChildren().usingWatcher(new DBWatcher).forPath(CATALOG_ROOT)

  /**
   * This lock ensures that there is only one
   * schema change going on at a time. Server's must acquire this
   * lock before attempting to initiate a schema change.
   */
  val schemaChangeLock = new InterProcessMutex(client, SCHEMA_LOCK_PATH)

  /**
   * schemaChangeBarrier blocks the call to update the schema until
   * all servers in the cluster have acknowledged the update. At this point,
   * it is safe for the client to query based on the new schema.
   */
  val schemaChangeBarrier = new DistributedCountdownLatch(
  client,
  SCHEMA_BARRIER_PATH)
  schemaChangeBarrier.start

  /**
   * Tell ZooKeeper to add this server to the cluster group. ZooKeeper will
   * do this and assign the server an ID that it will use in it's
   * network connection handshakes.
   * @param address the IP_ADDRESS:PORT of this servers backendConnection
   * @return The server ID assigned to by ZooKeeper
   */
  def registerWithZooKeeper(address: String): NetworkDestinationHandle = {
    require(!registered) // TODO will we ever want to re-register?
    val servername = client.create()
      // ephemeral so that if the servers gets disconnected, it will automatically
      // be removed from the group.
      .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
      .forPath(ZKPaths.makePath(CLUSTER_GROUP_NODE, "server-"), address.getBytes)
    // last 10 characters in name are sequence number - this provides the server ID
    initializeCache()
    registered = true
    getServerIdFromPath(servername)
  }

  // Ask ZooKeeper for IDs and addresses of all registered servers in the cluster,
  // and sets the local map.
  private def _getCurrentServers() = {
    serversInGroup = groupMembershipCache.getCurrentData.asScala.map(m => {
      val id = getServerIdFromPath(m.getPath)
      val addr = new String(m.getData).split(":")
      // Avoid creating an InetSocketAddress frequently, because creating
      // InetSocketAddress is expensive/blocking. (resolves DNS synchronously)
      (id, new InetSocketAddress(addr(0), addr(1).toInt))
    }).toMap
    logger.info("updated servers cache: " + serversInGroup)
  }

  private def initializeCache() = {
    groupMembershipCache = new PathChildrenCache(client, CLUSTER_GROUP_NODE, true)
    groupMembershipCache.getListenable.addListener(new PathChildrenCacheListener() {
      def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) {
        _getCurrentServers()
      }
    })
    // populate cache at initalization time
    groupMembershipCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE)
    _getCurrentServers()
  }

  def checkDBExistsZookeeper(db: String): Boolean = {
    try {
      client.checkExists()
        .forPath(makeDBPath(db))
      true
    } catch {
      case n: NoNodeException => false
    }
  }

  /**
   * Stat the node to check it's exists.
   * @param db The database the table belongs to
   * @param tbl The name of the table
   * @return True if it exists, false otherwise
   */
  def checkTableExistsZookeeper(db: String, tbl: String): Boolean = {
    try {
      client.checkExists()
//        .usingWatcher(new TableWatcher(db))
        .forPath(makeTablePath(db, tbl))
      true
    } catch {
      case n: NoNodeException => false
    }
  }

  /**
   * Returns the local map of servers in the cluster.
   * Must have registered with Zookeeper before calling this method.
   * @return map of (serverId, serverAddress) pairs corresponding to each server in cluster.
   *
   * @note This essentially returns the list of all servers taking part in the
   *       internal RPC service that are currently connected to Zookeeper.
   */
  def getServersInGroup(): Map[NetworkDestinationHandle, InetSocketAddress] = {
    // TODO (crankshaw) have a watcher on the cache to detect when servers
    // leave the group.
    if (!registered) {
      throw new IllegalStateException("Must register with group first")
    }
    serversInGroup
  }

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
        client.create().forPath(makeTablePath(dbName, tableName), schemaToBytes(schema))
      } else {
        client.create().forPath(makeDBPath(dbName))
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

  /**
   * Unused for now. Should be called when we start doing something more
   * sophisticated than pkill -9 java.
   */
  def shutdown {
    client.close()
  }

  /**
   * Watches for DB level changes to the system catalog
   */
  class DBWatcher extends CuratorWatcher {
    override def process(event: WatchedEvent) {
      // Find name of new DB and re-add watcher
      val catalogDBs = client.getChildren.usingWatcher(new DBWatcher).forPath(CATALOG_ROOT)
        .asScala
        .toSet
      val localDBs = Catalog.listLocalDatabases
      val diff = catalogDBs -- localDBs
      if (diff.size == 1) {
        val newDBName = diff.toList(0)
        Catalog._createDatabaseTrigger(newDBName)
        client.getChildren.usingWatcher(new TableWatcher(newDBName)).forPath(makeDBPath(newDBName))
      } else if (diff.size == 0) {
        // we already know about all the databases in the catalog.
        logger.warn("DB watcher activated but all tables accounted for")
      } else {
        throw new IllegalStateException(s"DB schema addition issue. DIFF = ${diff.mkString(",")}")
      }
      // set table watcher on new database
      schemaChangeBarrier.decrement()
    }
  } // end DBWatcher


  /**
   * Watches for changes to the tables of a specific database
   * @param dbname The database to watch
   */
  class TableWatcher(dbname: String) extends CuratorWatcher {
    override def process(event: WatchedEvent) {
      // Find name of new DB and re-add watcher
      val catalogTables = client.getChildren.usingWatcher(new TableWatcher(dbname))
        .forPath(ZKPaths.makePath(CATALOG_ROOT, dbname))
        .asScala
        .toSet
      val localTables = Catalog.listLocalTables(dbname)
      val diff = catalogTables -- localTables
      if (diff.size == 1) {
        val newTableName = diff.toList(0)
        val schemaBytes = client.getData.forPath(makeTablePath(dbname, newTableName))
        Catalog._createTableTrigger(dbname, newTableName, bytesToSchema(schemaBytes))
      } else if (diff.size == 0) {
        // we already know about all the tables in the catalog.
        logger.warn("Table watcher activated but all tables accounted for")
      } else {
        // TODO how should we handle this error?
        throw new IllegalStateException(s"Table Schema addition issue: DIFF = ${diff.mkString(",")}")
      }
      schemaChangeBarrier.decrement()
    }
  } // end TableWatcher


  /** Get the schema for the specified database and table
    *
    * @param db Database table exists in
    * @param table Table to get the schema for
    *
    * @return The specified schema
    */
  def getSchemaFor(db: String, table: String): Schema = {
    val schemaBytes = client.getData.forPath(makeTablePath(db, table))
    bytesToSchema(schemaBytes)
  }

  private def schemaToBytes(schema: Schema): Array[Byte] = {
    KryoThreadLocal.kryoTL.get().serialize(schema).array()
  }

  private def bytesToSchema(bytes: Array[Byte]): Schema = {
    KryoThreadLocal.kryoTL.get().deserialize(ByteBuffer.wrap(bytes)).asInstanceOf[Schema]
  }

} // end ZKClient

