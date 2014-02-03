package edu.berkeley.velox.cluster

import edu.berkeley.velox.datamodel.Row
import edu.berkeley.velox.NetworkDestinationHandle

trait Partitioner {
  def getMasterPartition(key: Row): NetworkDestinationHandle
}
