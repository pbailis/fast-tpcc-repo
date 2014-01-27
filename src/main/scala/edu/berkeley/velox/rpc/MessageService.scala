package edu.berkeley.velox.rpc

import edu.berkeley.velox.PartitionId
import edu.berkeley.velox.net.NetworkService

trait MessageService {
  var networkService: NetworkService = null
  def setNetworkService(networkService: NetworkService): Unit
  def receiveRemoteMessage(src: PartitionId, bytes: Array[Byte]): Unit
}
