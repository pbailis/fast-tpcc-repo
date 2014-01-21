package edu.berkeley.velox.cluster

import edu.berkeley.velox.datamodel.Key
import edu.berkeley.velox.PartitionId
import edu.berkeley.velox.conf.VeloxConfig

class RandomPartitioner extends Partitioner {
  override def getMasterPartition(key: Key): PartitionId = {
    return VeloxConfig.partitionList(key.value.hashCode() % VeloxConfig.partitionList.length)
  }
}