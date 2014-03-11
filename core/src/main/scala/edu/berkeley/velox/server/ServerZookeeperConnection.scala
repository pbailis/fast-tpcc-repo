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
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.util.zk.DistributedCountdownLatch
import edu.berkeley.velox.catalog.ServerCatalog
import edu.berkeley.velox.datamodel.{Schema, TableName, DatabaseName}
import edu.berkeley.velox.util.KryoThreadLocal
import java.nio.ByteBuffer
import scala.collection.JavaConverters._
import edu.berkeley.velox.server.{ZookeeperConnectionUtils => ZKUtils}



/**
 * This client is threadsafe. The CuratorFramework
 * and Recipes classes are threadsafe, and almost all of the mutable state is contained
 * in them. There should be one shared client per JVM.
 */
object ServerZookeeperConnection extends ZookeeperConnection {

  protected var registered = false


  // Start watching for changes in the catalog
  client.getChildren().usingWatcher(new ServerDBWatcher(client, schemaChangeBarrier)).forPath(ZKUtils.CATALOG_ROOT)



  /**
   * Tell ZooKeeper to add this server to the cluster group. ZooKeeper will
   * do this and assign the server an ID that it will use in it's
   * network connection handshakes.
   * @param address the IP_ADDRESS:PORT of this servers backendConnection
   * @return The server ID assigned to by ZooKeeper
   */
  def registerWithZooKeeper(address: ServerAddress): NetworkDestinationHandle = {
    require(!registered) // TODO will we ever want to re-register?
    val servername = client.create()
      // ephemeral so that if the servers gets disconnected, it will automatically
      // be removed from the group.
      .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
      .forPath(ZKPaths.makePath(ZKUtils.CLUSTER_GROUP_NODE, "server-"), ZookeeperConnectionUtils.addrToBytes(address))
    // last 10 characters in name are sequence number - this provides the server ID
    initializeCache(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE)
    registered = true
    ZKUtils.getServerIdFromPath(servername)
  }


  /**
   * Returns the local map of servers in the cluster.
   * Must have registered with Zookeeper before calling this method.
   * @return map of (serverId, serverAddress) pairs corresponding to each server in cluster.
   *
   * @note This essentially returns the list of all servers taking part in the
   *       internal RPC service that are currently connected to Zookeeper.
   */
  override def getServersInGroup(): Map[NetworkDestinationHandle, InetSocketAddress] = {
    // TODO (crankshaw) have a watcher on the cache to detect when servers
    // leave the group.
    if (!registered) {
      throw new IllegalStateException("Must register with group first")
    }
    super.getServersInGroup()
  }







} // end ZKClient

