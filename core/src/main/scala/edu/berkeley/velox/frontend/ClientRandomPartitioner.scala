package edu.berkeley.velox.frontend

import edu.berkeley.velox.cluster.Partitioner
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel.PrimaryKey
import edu.berkeley.velox._
import edu.berkeley.velox.datamodel.PrimaryKey


class ClientRandomPartitioner extends Partitioner with Logging {
  override def getMasterPartition(key: PrimaryKey): NetworkDestinationHandle = {
    val index = Math.abs(key.hashCode() % ClientZookeeperConnection.getServersInGroup().size)
    ClientZookeeperConnection.getServersInGroup().keys.toList(index)
  }
}
