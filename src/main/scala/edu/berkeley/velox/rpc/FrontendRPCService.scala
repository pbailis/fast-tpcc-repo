package edu.berkeley.velox.rpc

import edu.berkeley.velox.net.NIONetworkService
import edu.berkeley.velox.conf.VeloxConfig

/*
 * Used for all-to-all connections
 */

class FrontendRPCService extends MessageService {
  val name = "frontend"

  networkService = new NIONetworkService()
  networkService.setMessageService(this)
  
  override def initialize() {
    logger.debug(s"${VeloxConfig.partitionId} starting frontend RPC on port ${VeloxConfig.externalServerPort}")

    configureInboundListener(VeloxConfig.externalServerPort)
    networkService.start()

    logger.debug(s"${VeloxConfig.partitionId} frontend RPC listening on port ${VeloxConfig.externalServerPort}")

    Thread.sleep(VeloxConfig.bootstrapConnectionWaitSeconds * 1000)
    logger.debug(s"${VeloxConfig.partitionId} finished starting frontend RPC!")
  }
}