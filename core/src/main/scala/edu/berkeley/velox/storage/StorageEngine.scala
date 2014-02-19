package edu.berkeley.velox.storage

import java.util.Collection
import java.util.HashMap
import java.util.List
import java.util.Map
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel.{ItemKey, DataItem}
import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import java.util

class StorageEngine extends Logging {
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

            dataItems.remove(nextStamp)
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

  def getAll(keys: Collection[ItemKey]): util.HashMap[ItemKey, DataItem] = {
    val results = new util.HashMap[ItemKey, DataItem]
    for (key <- keys) {
      var item = getLatestItemForKey(key)
      if (item == null) item = DataItem.NULL
      results.put(key, item)
    }
    return results
  }

  def get(key: ItemKey): DataItem = {
    return getLatestItemForKey(key)
  }

  private def getByTimestamp(key: Nothing, requiredTimestamp: Long): DataItem = {
    val ret = getItemByVersion(key, requiredTimestamp)
    if (ret == null) logger.warn("No suitable value found for key " + key + " version " + requiredTimestamp)
    return ret
  }

  private def getLatestItemForKey(key: ItemKey): DataItem = {
    if (!latestGoodForKey.containsKey(key)) {
      return DataItem.NULL
    }

    return getItemByVersion(key, latestGoodForKey.get(key))
  }

  private def getItemByVersion(key: ItemKey, timestamp: Long): DataItem = {
    return dataItems.get(new KeyTimestampPair(key, timestamp))
  }

  def putAll(pairs: Map[ItemKey, DataItem]) {
    for (pair: Map.Entry[ItemKey, DataItem] <- pairs.entrySet) {
      put(pair.getKey, pair.getValue)
    }
  }

  def put(key: ItemKey, value: DataItem) {
    addItem(key, value)
    put_good(key, value.timestamp)
  }

  private def put_good(key: ItemKey, timestamp: Long): Boolean = {
    while (true) {
      val oldGood = latestGoodForKey.get(key)

      if (oldGood == 0) {
        if (latestGoodForKey.putIfAbsent(key, timestamp) == 0) {
          return true
        }
      }
      else if (oldGood < timestamp) {
        if (latestGoodForKey.replace(key, oldGood, timestamp)) {
          markForGC(key, oldGood)
          return true
        }
      }
      else {
        markForGC(key, timestamp)
        return false
      }
    }

    return false
  }

  def putPending(pairs: util.HashMap[ItemKey, DataItem]) {
    if (pairs.isEmpty) {
      logger.warn("put_pending of zero key value pairs?")
      return
    }
    val pendingPairs = new util.ArrayList[KeyTimestampPair](pairs.size)

    for (pair: Map.Entry[ItemKey, DataItem] <- pairs.entrySet) {
      addItem(pair.getKey, pair.getValue)
      pendingPairs.add(new KeyTimestampPair(pair.getKey, pair.getValue.timestamp))
    }
    val timestamp: Long = pairs.values.iterator.next.timestamp
    stampToPending.put(timestamp, pendingPairs)
  }

  def putGood(timestamp: Long): Map[String, DataItem] = {
    val ret = new HashMap[String, DataItem]

    val toUpdate = stampToPending.get(timestamp)
    if (toUpdate == null) {
      logger.error("No pending updates for timestamp " + timestamp)
      return ret
    }

    for (pair: KeyTimestampPair <- toUpdate) {
      val goodItem: DataItem = getItemByVersion(pair.key, pair.timestamp)
      put_good(pair.key, pair.timestamp)
    }

    stampToPending.remove(timestamp)
    return ret
  }

  private def addItem(key: ItemKey, value: DataItem) {
    dataItems.put(new KeyTimestampPair(key, value.timestamp), value)
  }

  private def markForGC(key: ItemKey, timestamp: Long) {
    if (true) return
    val stamp = new KeyTimestampPair(key, timestamp, System.currentTimeMillis+gcTimeMs)
    while (true) {
      try {
        candidatesForGarbageCollection.put(stamp)
        break
      }
      catch {
        case e: InterruptedException => {
          logger.error("Interrupted", e)
        }
      }
    }
  }

  private[storage] var dataItems = new ConcurrentHashMap[KeyTimestampPair, DataItem]
  private var latestGoodForKey = new ConcurrentHashMap[ItemKey, Long]
  private var stampToPending = new ConcurrentHashMap[Long, List[KeyTimestampPair]]
  private var candidatesForGarbageCollection = new LinkedBlockingQueue[KeyTimestampPair]
  val gcTimeMs = 5000
}


case class KeyTimestampPair(val key: ItemKey, val timestamp: Long) {
  var expirationTime = -1L

  def this(key: ItemKey, timestamp: Long, expirationTime: Long) {
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