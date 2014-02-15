package edu.berkeley.velox.cluster

import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.datamodel.ItemKey

trait Partitioner {
  def getMasterPartition(key: ItemKey): NetworkDestinationHandle
}
