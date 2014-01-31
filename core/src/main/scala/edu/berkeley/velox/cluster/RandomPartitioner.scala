package edu.berkeley.velox.cluster

import edu.berkeley.velox.datamodel.Key
import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.conf.VeloxConfig

class RandomPartitioner extends Partitioner {
  override def getMasterPartition(key: Key): NetworkDestinationHandle = {
    return VeloxConfig.partitionList(Math.abs(key.k.hashCode() % VeloxConfig.partitionList.length))
  }
}
