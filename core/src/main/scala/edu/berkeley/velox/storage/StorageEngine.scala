package edu.berkeley.velox.storage

import java.util.Collection
import java.util.HashMap
import java.util.List
import java.util.Map
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel.{PrimaryKey, Row}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import edu.berkeley.velox.benchmark.operation.DeferredIncrement
import edu.berkeley.velox.util.ConcurrentVeloxHashMap
import edu.berkeley.velox.conf.VeloxConfig

class StorageEngine extends Logging {

  val numLocks = 100
   val locks = new Array[AtomicBoolean](numLocks)

   for(i <- 0 until numLocks) {
     locks(i) = new AtomicBoolean()
   }

   def spinlock_lock(key: PrimaryKey) {
     val lock = locks(key.hashCode % locks.length)
     while(lock.compareAndSet(false, true)){}
   }

   def spinlock_unlock(key: PrimaryKey) {
     val lock = locks(key.hashCode % locks.length)
     lock.set(false)
   }

  def initialize() {
    new Thread(new Runnable {
      def run {
        var currentTime: Long = -1
        var nextStamp: KeyTimestampPair = null
        while (true) {
          try {
            if (nextStamp == null)
              nextStamp = candidatesForGarbageCollection.take

            if (nextStamp.expirationTime > currentTime) {
              Thread.sleep(nextStamp.expirationTime - currentTime)
            } else {
              currentTime = System.currentTimeMillis()
              if(nextStamp.expirationTime > currentTime)
                Thread.sleep(nextStamp.expirationTime - currentTime)
            }

            dataItems.put(nextStamp, null)
            nextStamp = null
          }
          catch {
            case e: InterruptedException => {
            }
          }
        }
      }
    }, "Storage-GC-Thread").start
  }

  def getAll(values: util.Map[PrimaryKey, Row]): util.Map[PrimaryKey, Row] = {
    val it = values.entrySet.iterator()
    while(it.hasNext) {
      val pk_pair = it.next()

      val latestVal = latestGoodForKey.get(pk_pair.getKey, null)

      if(latestVal != null) {
        val c_it = pk_pair.getValue.columns.entrySet.iterator()
        while(c_it.hasNext) {
          val col = c_it.next().getKey
          val v = latestVal.readColumn(col)
          if(v != null) {
            pk_pair.getValue.column(col, v)
          }
        }
      }
    }

    values
  }

  def get(key: PrimaryKey): Row = {
    return getLatestItemForKey(key)
  }

  private def getByTimestamp(key: Nothing, requiredTimestamp: Long): Row = {
    val ret = getItemByVersion(key, requiredTimestamp)
    if (ret == null) logger.warn("No suitable value found for key " + key + " version " + requiredTimestamp)
    return ret
  }

  private def getLatestItemForKey(key: PrimaryKey): Row = {
    latestGoodForKey.get(key, Row.NULL)
  }

  private def getItemByVersion(key: PrimaryKey, timestamp: Long): Row = {
    return dataItems.get(new KeyTimestampPair(key, timestamp), Row.NULL)
  }

  def putAll(pairs: util.Map[PrimaryKey, Row]) {
    val it = pairs.entrySet().iterator()
    while(it.hasNext) {
      val pair = it.next()
      put(pair.getKey, pair.getValue)
    }
  }

  def put(key: PrimaryKey, value: Row) {
    addItem(key, value)
    put_good(key, value)
  }

  private def put_good(key: PrimaryKey, good: Row): Boolean = {
    while (true) {
      val oldGood = latestGoodForKey.get(key, null)

      if (oldGood == null) {
        if (latestGoodForKey.putIfAbsent(key, good, null) == null) {
          return true
        }
      }
      else if (oldGood.timestamp < good.timestamp) {
        if (latestGoodForKey.replace(key, oldGood, good, null)) {
          markForGC(key, oldGood.timestamp)
          return true
        }
      }
      else {
        markForGC(key, good.timestamp)
        return false
      }
    }

    return false
  }

  def putPending(pairs: util.Map[PrimaryKey, Row]) {
    if (pairs.isEmpty) {
      logger.warn("put_pending of zero key value pairs?")
      return
    }
    val pendingPairs = new util.ArrayList[KeyRowPair](pairs.size)

    val it = pairs.entrySet().iterator()
    while(it.hasNext) {
      val pair = it.next()
      addItem(pair.getKey, pair.getValue)
      pendingPairs.add(new KeyRowPair(pair.getKey, pair.getValue))
    }

    val timestamp: Long = pairs.values.iterator.next.timestamp
    stampToPending.put(timestamp, pendingPairs)
  }

  def putGood(timestamp: Long, deferredIncrement: DeferredIncrement = null): Integer = {
    val toUpdate = stampToPending.get(timestamp, null)

    var ret = -1

    if (toUpdate == null) {
      logger.error("No pending updates for timestamp " + timestamp)
      return ret
    }

    val it = toUpdate.iterator()
    while(it.hasNext) {
      val pair = it.next()
      val goodItem: Row = pair.row
      put_good(pair.key, goodItem)
    }

    if(deferredIncrement != null) {
      val goodRow = new Row

      latestGoodForKey.put(deferredIncrement.destinationKey, goodRow)
      spinlock_lock(deferredIncrement.counterKey)

      val toInc = getLatestItemForKey(deferredIncrement.counterKey)
      val curVal = toInc.readColumn(deferredIncrement.counterColumn).asInstanceOf[Integer]
      val newVal = curVal+1
      toInc.column(deferredIncrement.counterColumn, newVal)
      goodRow.column(deferredIncrement.destinationColumn, newVal)
      spinlock_unlock(deferredIncrement.counterKey)
      ret = newVal
    }

    stampToPending.put(timestamp, null)
    return ret
  }

  private def addItem(key: PrimaryKey, value: Row) {
    dataItems.put(new KeyTimestampPair(key, value.timestamp), value)
  }

  private def markForGC(key: PrimaryKey, timestamp: Long) {
    if (true) return
    val stamp = new KeyTimestampPair(key, timestamp, System.currentTimeMillis+gcTimeMs)

    var done = false

    while (!done) {
      try {
        candidatesForGarbageCollection.put(stamp)
        done = true
      }
      catch {
        case e: InterruptedException => {
          logger.error("Interrupted", e)
        }
      }
    }
  }

  def numKeys: Int = { dataItems.size }

  private[storage] var dataItems = new ConcurrentVeloxHashMap[KeyTimestampPair, Row](VeloxConfig.storage_size, VeloxConfig.storage_parallelism, "dataItems")
  private var latestGoodForKey = new ConcurrentVeloxHashMap[PrimaryKey, Row](VeloxConfig.storage_size, VeloxConfig.storage_parallelism, "latestGoodForKey")
  private var stampToPending = new ConcurrentVeloxHashMap[Long, List[KeyRowPair]](VeloxConfig.storage_size, VeloxConfig.storage_parallelism, "stampToPending")
  private var candidatesForGarbageCollection = new LinkedBlockingQueue[KeyTimestampPair]
  val gcTimeMs = 5000
}


case class KeyRowPair(val key: PrimaryKey, val row: Row)

case class KeyTimestampPair(val key: PrimaryKey, val timestamp: Long) {
  var expirationTime = -1L

  def this(key: PrimaryKey, timestamp: Long, expirationTime: Long) {
    this(key, timestamp)
    this.expirationTime = expirationTime
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case kts: KeyTimestampPair => key == kts.key && timestamp == kts.timestamp
      case _ => false
    }
  }

  override def hashCode: Int = {
    return key.hashCode * timestamp.hashCode()
  }

}
