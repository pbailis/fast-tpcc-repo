package edu.berkeley.velox.cluster

import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.server.ZKClient
import edu.berkeley.velox.datamodel.PrimaryKey

// We give the partitioner a reference to the zkConfig so that it knows
// the current list of servers in the cluster
class RandomPartitioner(zkConfig: ZKClient) extends Partitioner {
  override def getMasterPartition(key: PrimaryKey): NetworkDestinationHandle = {
    assume(zkConfig != null)
    val index = Math.abs(key.hashCode() % zkConfig.getServersInGroup().size)
    zkConfig.getServersInGroup().keys.toList(index)
  }
}
