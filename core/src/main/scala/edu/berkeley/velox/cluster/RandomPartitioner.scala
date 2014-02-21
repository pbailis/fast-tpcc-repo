package edu.berkeley.velox.cluster

import edu.berkeley.velox.datamodel.Key
import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.server.ZKClient

// We give the partitioner a reference to the zkConfig so that it knows
// the current list of servers in the cluster
class RandomPartitioner(zkConfig: ZKClient) extends Partitioner {
  override def getMasterPartition(key: Key): NetworkDestinationHandle = {
    assume(zkConfig != null)
    val index = Math.abs(key.k.hashCode() % zkConfig.getServersInGroup().size)
    // TODO: Kind of a hack, we should fix when we figure out dynamic cluster membership
    zkConfig.getServersInGroup().keys.toList(index)
  }
}
