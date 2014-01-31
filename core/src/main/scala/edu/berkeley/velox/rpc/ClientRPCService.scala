package edu.berkeley.velox.rpc

import edu.berkeley.velox.net.NIONetworkService
import edu.berkeley.velox.conf.VeloxConfig
import java.net.InetSocketAddress

/*
 * Used for all-to-all connections
 */

class ClientRPCService(val frontendServerAddresses: Iterable[InetSocketAddress]) extends MessageService {
  val name = "client"

  networkService = new NIONetworkService
  networkService.setMessageService(this)
  
  override def initialize() {
    frontendServerAddresses.foreach( {
      address => connect(address)
    })

    networkService.start()
  }
}