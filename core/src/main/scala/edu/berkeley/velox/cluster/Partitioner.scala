package edu.berkeley.velox.cluster

import edu.berkeley.velox.datamodel.Key
import edu.berkeley.velox.NetworkDestinationHandle

// TODO the partitioner should probably have a complete list of current server
// IDs in the cluster so it can partition between them correctly
trait Partitioner {
  def getMasterPartition(key: Key): NetworkDestinationHandle
}
