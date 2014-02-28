package edu.berkeley.velox.rpc

import edu.berkeley.velox.conf.VeloxConfig
import java.net.InetSocketAddress
import scala.util.Random

/*
 * Used for all-to-all connections
 */

class ClientRPCService(val frontendServerAddresses: Iterable[InetSocketAddress]) extends MessageService {
  val name = "client"

  VeloxConfig.partitionId = -Math.abs(Random.nextInt())

  networkService = VeloxConfig.getNetworkService(true, true, serverID = VeloxConfig.partitionId)
  networkService.setMessageService(this)
  
  override def initialize() {
    frontendServerAddresses.foreach( {
      address => connect(address)
    })

    networkService.start()
  }
}
