package edu.berkeley.velox.rpc

import edu.berkeley.velox.conf.VeloxConfig

/*
 * Used for all-to-all connections
 */

class FrontendRPCService extends MessageService {
  val name = "frontend"
  serviceID = VeloxConfig.partitionId

  networkService = VeloxConfig.getNetworkService()
  networkService.setMessageService(this)
  
  override def initialize() {
    logger.info(s"${VeloxConfig.partitionId} starting frontend RPC on port ${VeloxConfig.externalServerPort}")

    configureInboundListener(VeloxConfig.externalServerPort)
    networkService.start()

    logger.info(s"${VeloxConfig.partitionId} frontend RPC listening on port ${VeloxConfig.externalServerPort}")

    Thread.sleep(VeloxConfig.bootstrapConnectionWaitSeconds * 1000)
    logger.info(s"${VeloxConfig.partitionId} finished starting frontend RPC!")
  }
}
