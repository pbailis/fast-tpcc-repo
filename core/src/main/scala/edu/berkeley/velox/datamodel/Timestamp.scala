package edu.berkeley.velox.datamodel

import edu.berkeley.velox.conf.VeloxConfig
import com.typesafe.scalalogging.slf4j.Logging

object Timestamp extends Logging {
  val NO_TIMESTAMP = -1L
  @volatile var latestMillis = -1L
  @volatile var sequenceNo = 0;

  def assignNewTimestamp(): Long = {
    var chosenTime = System.currentTimeMillis()
    var chosenSeqNo = 0;

     synchronized {
         if (latestMillis < chosenTime) {
             latestMillis = chosenTime;
             sequenceNo = 0;
             chosenSeqNo = 0;
         } else if (latestMillis == chosenTime) {
             sequenceNo += 1
             chosenSeqNo = sequenceNo;
         } else {
             chosenTime = latestMillis;
             sequenceNo += 1
             chosenSeqNo = sequenceNo;
         }
     }

     return (chosenTime << 26) | (chosenSeqNo << 12) | (VeloxConfig.partitionId);  }
}
