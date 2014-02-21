package edu.berkeley.velox.cluster

import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.datamodel.PrimaryKey

trait Partitioner {
  def getMasterPartition(key: PrimaryKey): NetworkDestinationHandle
}
