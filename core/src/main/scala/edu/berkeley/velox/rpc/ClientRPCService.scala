package edu.berkeley.velox.rpc

import edu.berkeley.velox.conf.VeloxConfig
import java.net.InetSocketAddress
import edu.berkeley.velox.frontend.ClientZookeeperConnection
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.CuratorFramework
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.server.ZookeeperConnectionUtils
import edu.berkeley.velox.NetworkDestinationHandle


/*
 * Used for all-to-all connections
 */

// class ClientRPCService(val serverAddresses: Iterable[InetSocketAddress]) extends MessageService {
class ClientRPCService extends MessageService {
  val name = "client"

  networkService = VeloxConfig.getNetworkService(name)
  networkService.setMessageService(this)
  
  override def initialize() {

    ClientZookeeperConnection.addGroupMembershipListener(new MyPathChildrenCacheListener(this))
    ClientZookeeperConnection.initializeClientGroupMembershipCache()
    networkService.start()
    Thread.sleep(VeloxConfig.bootstrapConnectionWaitSeconds * 1000)
    // block until we have connected to all servers
    networkService.blockForConnections(VeloxConfig.expectedNumInternalServers)
  }
}


class MyPathChildrenCacheListener(ms: ClientRPCService) extends PathChildrenCacheListener with Logging{
  override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) {
    event.getType match {
      case PathChildrenCacheEvent.Type.CHILD_ADDED =>
        val handle = ZookeeperConnectionUtils.getServerIdFromPath(event.getData.getPath)
        addConnection(handle, event.getData.getData)
        logger.debug(s"Client adding connection to $handle in CHILD_ADDED case")
      case PathChildrenCacheEvent.Type.INITIALIZED => logger.debug("Group membership cache initialized")
      case other => logger.info(s"ChildEvent of type ${event.getType} detected")
    }
  }

  def addConnection(handle: NetworkDestinationHandle, rawAddr: Array[Byte]) {
    val addrSplits = ZookeeperConnectionUtils.bytesToAddr(rawAddr)
    val address = new InetSocketAddress(addrSplits.ipAddr, addrSplits.frontendPort)
    ms.connect(handle, address)
  }
}