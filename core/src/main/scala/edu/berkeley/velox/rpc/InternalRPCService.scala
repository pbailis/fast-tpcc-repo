package edu.berkeley.velox.rpc

import edu.berkeley.velox.conf.VeloxConfig


/*
 * Used for all-to-all connections
 */

class InternalRPCService extends MessageService {
  val name = "internal"
  serviceID = VeloxConfig.partitionId

  networkService = VeloxConfig.getNetworkService(true,
    tcpNoDelay = VeloxConfig.tcpNoDelay,
    serverID = VeloxConfig.partitionId)
  networkService.setMessageService(this)

  override def initialize() {
    logger.info(s"${VeloxConfig.partitionId} starting internal RPC acceptor on port ${VeloxConfig.internalServerPort}")
    configureInboundListener(VeloxConfig.internalServerPort);
    networkService.start()
    logger.info(s"${VeloxConfig.partitionId} internal RPC acceptor listening on port ${VeloxConfig.internalServerPort}")

    Thread.sleep(VeloxConfig.bootstrapConnectionWaitSeconds * 1000)

    // connect to all higher-numbered partitions
    VeloxConfig.internalServerAddresses.filter {
      case (id, addr) => id > VeloxConfig.partitionId
    }.foreach {
      case (partitionId, remoteAddress) =>
        logger.trace(s"${VeloxConfig.partitionId} connecting to ID $partitionId")
        connect(partitionId, remoteAddress)
    }

    networkService.blockForConnections(VeloxConfig.internalServerAddresses.keys.size-1)
    logger.info(s"${VeloxConfig.partitionId} started internal RPC!")


    if(VeloxConfig.thread_handler) {
      executor = networkService.executor;
    }

  }
}
