package edu.berkeley.velox.rpc

import edu.berkeley.velox.net.{ArrayNetworkService,NIONetworkService}
import edu.berkeley.velox.conf.VeloxConfig
import java.net.InetSocketAddress

/*
 * Used for all-to-all connections
 */

class ClientRPCService(val frontendServerAddresses: Iterable[InetSocketAddress]) extends MessageService {
  val name = "client"

  networkService = VeloxConfig.getNetworkService(name)
  networkService.setMessageService(this)
  
  override def initialize() {
    frontendServerAddresses.foreach( {
      address => connect(address)
    })

    networkService.start()
  }
}
