package edu.berkeley.velox.cluster

import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.server.ServerZookeeperConnection
import edu.berkeley.velox.datamodel.PrimaryKey
import edu.berkeley.velox.frontend.ClientZookeeperConnection
import com.typesafe.scalalogging.slf4j.Logging

// We give the partitioner a reference to the zkConfig so that it knows
// the current list of servers in the cluster
class ServerRandomPartitioner extends Partitioner {
  override def getMasterPartition(key: PrimaryKey): NetworkDestinationHandle = {
    val index = Math.abs(key.hashCode() % ServerZookeeperConnection.getServersInGroup().size)
    ServerZookeeperConnection.getServersInGroup().keys.toList(index)
  }
}

