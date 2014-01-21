package edu.berkeley.velox.cluster

import edu.berkeley.velox.datamodel.Key
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.PartitionId

trait Partitioner {
  def getMasterPartition(key: Key): PartitionId
}