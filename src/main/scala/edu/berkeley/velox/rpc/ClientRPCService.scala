package edu.berkeley.velox.rpc

import edu.berkeley.velox.net.NIONetworkService
import edu.berkeley.velox.conf.VeloxConfig

/*
 * Used for all-to-all connections
 */

class ClientRPCService extends MessageService {
  val name = "client"

  networkService = new NIONetworkService
  networkService.setMessageService(this)
  
  override def initialize() {
    VeloxConfig.frontendServerAddresses.foreach( {
      case (handle, address) => connect(address)
    })

    networkService.start()
  }
}