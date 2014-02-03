package edu.berkeley.velox.cluster

import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.datamodel.Row

class RandomPartitioner extends Partitioner {
  override def getMasterPartition(key: Row): NetworkDestinationHandle = {
    return VeloxConfig.partitionList(Math.abs(key.hashCode() % VeloxConfig.partitionList.length))
  }
}
