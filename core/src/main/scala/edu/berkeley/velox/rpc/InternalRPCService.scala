package edu.berkeley.velox.rpc

import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.NetworkDestinationHandle
import java.net.InetSocketAddress


/*
 * Used for all-to-all connections
 */

class InternalRPCService(partitionId: NetworkDestinationHandle,
                         internalServerAddresses: Map[NetworkDestinationHandle, InetSocketAddress]) extends MessageService {
  val name = "internal"
  serviceID = partitionId

  networkService = VeloxConfig.getNetworkService(name, true,
    tcpNoDelay = VeloxConfig.tcpNoDelay,
    serverID = partitionId)
  networkService.setMessageService(this)

  override def initialize() {
    logger.info(s"$partitionId starting internal RPC acceptor on port ${VeloxConfig.internalServerPort}")
    configureInboundListener(VeloxConfig.internalServerPort);
    networkService.start()
    logger.info(s"$partitionId internal RPC acceptor listening on port ${VeloxConfig.internalServerPort}")

    Thread.sleep(VeloxConfig.bootstrapConnectionWaitSeconds * 1000)

    // Connect to all lower-numbered partitions. This is a more
    // compatible policy when letting zookeeper do ID assignment.
    internalServerAddresses.filter {
      case (id, addr) => id < partitionId
    }.foreach {
      case (remotePartitionId, remoteAddress) =>
        logger.trace(s"$partitionId} connecting to ID $remotePartitionId")
        connect(remotePartitionId, remoteAddress)
    }

    // we still block until all servers that we know about have connected to us
    networkService.blockForConnections(VeloxConfig.expectedNumInternalServers-1)
    logger.info(s"$partitionId} started internal RPC!")
  }
}
