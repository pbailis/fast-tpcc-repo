package edu.berkeley.velox.cluster

import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.benchmark.{TPCCConstants, TPCCItemKey}
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.datamodel.PrimaryKey
import com.typesafe.scalalogging.slf4j.Logging

/**
 * Created by pbailis on 2/14/14.
 */

class TPCCPartitioner extends Partitioner with Logging {
  val partitions = VeloxConfig.partitionList

  override def getMasterPartition(ikey: PrimaryKey): NetworkDestinationHandle = {
    if (ikey.table == TPCCConstants.ITEM_TABLE) {
       return partitions(VeloxConfig.partitionId)
    } else {
      return partitions((ikey.keyColumns(0) - 1) % partitions.size)
    }
  }
}
