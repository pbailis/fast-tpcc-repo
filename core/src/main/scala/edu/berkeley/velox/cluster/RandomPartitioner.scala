package edu.berkeley.velox.cluster

import edu.berkeley.velox.Key
import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.conf.VeloxConfig

class RandomPartitioner extends Partitioner {
  override def getMasterPartition(key: Key): NetworkDestinationHandle = {
    return VeloxConfig.partitionList(key.k.hashCode() % VeloxConfig.partitionList.length)
  }
}
