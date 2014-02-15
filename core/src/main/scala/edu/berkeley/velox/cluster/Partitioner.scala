package edu.berkeley.velox.cluster

import edu.berkeley.velox.datamodel.{PrimaryKey, Row}
import edu.berkeley.velox.NetworkDestinationHandle

trait Partitioner {
  def getMasterPartition(key: PrimaryKey): NetworkDestinationHandle
}
