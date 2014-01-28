package edu.berkeley.velox.rpc

import edu.berkeley.velox.net.NIONetworkService
import edu.berkeley.velox.conf.VeloxConfig


/*
 * Used for all-to-all connections
 */

class InternalRPCService extends MessageService {
  val name = "internal"

  networkService = new NIONetworkService(performIDHandshake = true)
  networkService.setMessageService(this)
  
  override def initialize() {
    logger.debug(s"${VeloxConfig.partitionId} starting internal RPC acceptor on port ${VeloxConfig.internalServerPort}")
    configureInboundListener(VeloxConfig.internalServerPort);
    networkService.start()
    logger.debug(s"${VeloxConfig.partitionId} internal RPC acceptor listening on port ${VeloxConfig.internalServerPort}")

    Thread.sleep(VeloxConfig.bootstrapConnectionWaitSeconds * 1000)

    // connect to all higher-numbered partitions
    VeloxConfig.internalServerAddresses.filter {
      case (id, addr) => id > VeloxConfig.partitionId
    }.foreach {
      case (partitionId, remoteAddress) =>
        logger.trace(s"${VeloxConfig.partitionId} connecting to ID $partitionId")
        connect(partitionId, remoteAddress)
    }

    Thread.sleep(VeloxConfig.bootstrapConnectionWaitSeconds * 1000)
    logger.debug(s"${VeloxConfig.partitionId} started internal RPC!")
  }
}