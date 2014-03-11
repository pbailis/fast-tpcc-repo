package edu.berkeley.velox.server

import org.apache.curator.utils.ZKPaths
import edu.berkeley.velox._
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener, PathChildrenCache}
import java.net.InetSocketAddress
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import edu.berkeley.velox.conf.VeloxConfig
import org.apache.curator.retry.ExponentialBackoffRetry
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.server.{ZookeeperConnectionUtils => ZKUtils}
import edu.berkeley.velox.datamodel.{TableName, DatabaseName, Schema}
import edu.berkeley.velox.util.KryoThreadLocal
import java.nio.ByteBuffer
import org.apache.zookeeper.KeeperException.{NodeExistsException, NoNodeException}
import edu.berkeley.velox.util.zk.DistributedCountdownLatch
import org.apache.zookeeper.CreateMode


/**
 * Created by crankshaw on 3/4/14.
 */
trait ZookeeperConnection extends Logging {



  @volatile protected var serversInGroup = Map[NetworkDestinationHandle, InetSocketAddress]().empty

  protected val client = CuratorFrameworkFactory.builder()
    .namespace(ZKUtils.VELOX_NAMESPACE)
    .connectString(VeloxConfig.zookeeperServerAddresses)
    .retryPolicy(new ExponentialBackoffRetry(1000, 10))
    .build()
  client.start() // clients must be started before they can be used

  try {
    client.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .forPath(ZKUtils.CLUSTER_GROUP_NODE)
  } catch {
    case e: NodeExistsException => logger.info("No need to create group node, already exists.")
  }

  try {
    client.create()
      .withMode(CreateMode.PERSISTENT)
      .forPath(ZKUtils.CATALOG_ROOT)
  } catch {
    case e: NodeExistsException => logger.info("Someone else already created catalog")
  }


  protected var groupMembershipCache = new PathChildrenCache(client, ZKUtils.CLUSTER_GROUP_NODE, true)
  //null.asInstanceOf[PathChildrenCache]

  def listZookeeperDBs(): List[DatabaseName] = {
    client.getChildren.forPath(ZKUtils.CATALOG_ROOT).asScala.toList
  }

  def listZookeeperTables(db: DatabaseName): Map[TableName, Schema] = {
    logger.error(s"getting children for database path ${ZKUtils.makeDBPath(db)}")
    val tables = client.getChildren.forPath(ZKUtils.makeDBPath(db)).asScala
    val ret = tables.map { t => {
      val schema = getSchemaFor(db, t)
      (t, schema)
    }
    }.toMap
    logger.error(s"Listing ZK tables: $tables, $ret")
    ret
  }

  /**
   * schemaChangeBarrier blocks the call to update the schema until
   * all servers in the cluster have acknowledged the update. At this point,
   * it is safe for the client to query based on the new schema.
   */
  val schemaChangeBarrier = new DistributedCountdownLatch(
    client,
    ZKUtils.SCHEMA_BARRIER_PATH)
  schemaChangeBarrier.start

  // Ask ZooKeeper for IDs and addresses of all registered servers in the cluster,
  // and sets the local map.
  protected def _getCurrentServers() = {
    serversInGroup = groupMembershipCache.getCurrentData.asScala.map(m => {
      val id = ZKUtils.getServerIdFromPath(m.getPath)
      val addr = ZookeeperConnectionUtils.bytesToAddr(m.getData)
      // Avoid creating an InetSocketAddress frequently, because creating
      // InetSocketAddress is expensive/blocking. (resolves DNS synchronously)
      (id, new InetSocketAddress(addr.ipAddr, addr.internalPort))
    }).toMap
    logger.debug("updated servers cache: " + serversInGroup)
  }

  /** Get the schema for the specified database and table
    *
    * @param db Database table exists in
    * @param table Table to get the schema for
    *
    * @return The specified schema
    */
  def getSchemaFor(db: String, table: String): Schema = {
    val schemaBytes = client.getData.forPath(ZKUtils.makeTablePath(db, table))
    ZKUtils.bytesToSchema(schemaBytes)
  }


  protected def initializeCache(startMode: PathChildrenCache.StartMode) = {
    groupMembershipCache.getListenable.addListener(new PathChildrenCacheListener() {
      def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) {
        _getCurrentServers()
      }
    })
    // populate cache at initalization time
    groupMembershipCache.start(startMode)
    _getCurrentServers()
  }



  def checkDBExistsZookeeper(db: String): Boolean = {
    try {
      client.checkExists()
        .forPath(ZKUtils.makeDBPath(db))
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
        .forPath(ZKUtils.makeTablePath(db, tbl))
      true
    } catch {
      case n: NoNodeException => false
    }
  }

  def getServersInGroup(): Map[NetworkDestinationHandle, InetSocketAddress] = {
    serversInGroup
  }

  /**
   * Unused for now. Should be called when we start doing something more
   * sophisticated than pkill -9 java.
   */
  def shutdown {
    client.close()
  }


}

object ZookeeperConnectionUtils {
  val VELOX_NAMESPACE = "velox"
  val ZK_UTIL_PATH = "/velox-utils"
  val CATALOG_ROOT = "/catalog"
  val SCHEMA_BARRIER_PATH = ZKPaths.makePath(ZK_UTIL_PATH, "schema-barrier")
  val SCHEMA_LOCK_PATH = ZKPaths.makePath(ZK_UTIL_PATH, "schema-change-lock")
  val CLUSTER_GROUP_NODE = ZKPaths.makePath(ZK_UTIL_PATH, "velox-cluster")

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

  def schemaToBytes(schema: Schema): Array[Byte] = {
    KryoThreadLocal.kryoTL.get().serialize(schema).array()
  }

  def bytesToSchema(bytes: Array[Byte]): Schema = {
    KryoThreadLocal.kryoTL.get().deserialize(ByteBuffer.wrap(bytes)).asInstanceOf[Schema]
  }

  def addrToBytes(addr: ServerAddress): Array[Byte] = {
    KryoThreadLocal.kryoTL.get().serialize(addr).array()
  }

  def bytesToAddr(bytes: Array[Byte]): ServerAddress = {
    KryoThreadLocal.kryoTL.get().deserialize(ByteBuffer.wrap(bytes)).asInstanceOf[ServerAddress]
  }
}

class ServerAddress(val ipAddr: String, val internalPort: Int, val frontendPort: Int)