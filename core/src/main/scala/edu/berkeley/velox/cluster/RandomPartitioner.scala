package edu.berkeley.velox.cluster

import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.server.ZKClient
import edu.berkeley.velox.datamodel.PrimaryKey

// We give the partitioner a reference to the zkConfig so that it knows
// the current list of servers in the cluster
class RandomPartitioner extends Partitioner {
  override def getMasterPartition(key: PrimaryKey): NetworkDestinationHandle = {
    val index = Math.abs(key.hashCode() % ZKClient.getServersInGroup().size)
    ZKClient.getServersInGroup().keys.toList(index)
  }
}
