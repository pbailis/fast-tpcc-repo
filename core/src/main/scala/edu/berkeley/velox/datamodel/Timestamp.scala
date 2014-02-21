package edu.berkeley.velox.datamodel

import edu.berkeley.velox.conf.VeloxConfig
import com.typesafe.scalalogging.slf4j.Logging
import java.util.concurrent.atomic.AtomicInteger

object Timestamp {
  val NO_TIMESTAMP = -1L

  val threadID = new AtomicInteger

  val timestampThreadLocal = new ThreadLocal[Timestamp]() {
    override protected def initialValue(): Timestamp = {
      val tid = threadID.incrementAndGet()
      assert(tid < 32)
      new Timestamp(tid)
    }
  }

  def assignNewTimestamp(): Long = {
    timestampThreadLocal.get().assignNewTimestamp()
  }
}

class Timestamp(val threadID: Int) {
  @volatile var latestMillis = -1L
  @volatile var sequenceNo = 0;

  def assignNewTimestamp(): Long = {
    var chosenTime = System.currentTimeMillis()
    var chosenSeqNo = 0;

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

     return (chosenTime << 32) | (chosenSeqNo << 18) | (VeloxConfig.partitionId << 6) | threadID ;  }
}
