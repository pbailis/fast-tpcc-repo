package edu.berkeley.velox.rpc

import edu.berkeley.velox.net.{ArrayNetworkService,NIONetworkService}
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.NetworkDestinationHandle

/*
 * Used for all-to-all connections
 */

class FrontendRPCService(partitionId: NetworkDestinationHandle) extends MessageService {
  val name = "frontend"
  serviceID = partitionId

  networkService = VeloxConfig.getNetworkService(name)
  networkService.setMessageService(this)
  
  override def initialize() {
    logger.info(s"${VeloxConfig.serverIpAddress} starting frontend RPC on port ${VeloxConfig.externalServerPort}")

    configureInboundListener(VeloxConfig.externalServerPort)
    networkService.start()

    logger.info(s"${VeloxConfig.serverIpAddress} frontend RPC listening on port ${VeloxConfig.externalServerPort}")

    Thread.sleep(VeloxConfig.bootstrapConnectionWaitSeconds * 1000)
    logger.info(s"${VeloxConfig.serverIpAddress} finished starting frontend RPC!")
  }
}
