package edu.berkeley.velox.cluster

import edu.berkeley.velox.Key
import edu.berkeley.velox.NetworkDestinationHandle

trait Partitioner {
  def getMasterPartition(key: Key): NetworkDestinationHandle
}
