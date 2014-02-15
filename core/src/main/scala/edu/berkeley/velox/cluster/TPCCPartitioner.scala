package edu.berkeley.velox.cluster

import edu.berkeley.velox.datamodel.ItemKey
import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.benchmark.{TPCCConstants, TPCCItemKey}
import edu.berkeley.velox.conf.VeloxConfig

/**
 * Created by pbailis on 2/14/14.
 */

class TPCCPartitioner extends Partitioner {
  val partitions = VeloxConfig.partitionList

  override def getMasterPartition(ikey: ItemKey): NetworkDestinationHandle = {
    val key = ikey.asInstanceOf[TPCCItemKey]

    if (key.table == TPCCConstants.ITEM_TABLE) {
       return partitions(VeloxConfig.partitionId)
    } else {
      return partitions((key.w_id - 1) % partitions.size)
    }
  }
}
