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

  def getAll(values: util.HashMap[PrimaryKey, Row]): util.HashMap[PrimaryKey, Row] = {
    val it = values.entrySet.iterator()
    while(it.hasNext) {
      val pk_pair = it.next()

      val table = latestGoodForKey.get(pk_pair.getKey.table)

      if(table != null) {
        val latestVal = table.get(pk_pair.getKey)

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
    val table = dataItems.get(key.table)

    if(table == null)
      return null

    val ret = table.get(key)

    if(ret == null) {
      return Row.NULL
    }

    ret
  }

  private def getItemByVersion(key: PrimaryKey, timestamp: Long): Row = {
    val table = dataItems.get(key.table)

    if(table == null)
      return null

    return table.get(new KeyTimestampPair(key, timestamp))
  }

  def putAll(pairs: Map[PrimaryKey, Row]) {
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
      var table = latestGoodForKey.get(key.table)

      if(table == null) {
        latestGoodForKey.put(key.table, new ConcurrentHashMap[PrimaryKey, Row](10000000, .2f, 36))
        table = latestGoodForKey.get(key.table)
      }

      val oldGood = table.get(key)

      if (oldGood == null) {
        if (table.putIfAbsent(key, good) == null) {
          return true
        }
      }
      else if (oldGood.timestamp < good.timestamp) {
        if (table.replace(key, oldGood, good)) {
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

  def putPending(pairs: util.HashMap[PrimaryKey, Row]) {
    if (pairs.isEmpty) {
      logger.warn("put_pending of zero key value pairs?")
      return
    }
    val pendingPairs = new util.ArrayList[KeyTimestampPair](pairs.size)

    val it = pairs.entrySet().iterator()
    while(it.hasNext) {
      val pair = it.next()
      addItem(pair.getKey, pair.getValue)
      pendingPairs.add(new KeyTimestampPair(pair.getKey, pair.getValue.timestamp))
    }

    val timestamp: Long = pairs.values.iterator.next.timestamp
    stampToPending.put(timestamp, pendingPairs)
  }

  def putGood(timestamp: Long): Map[String, Row] = {
    val ret = new HashMap[String, Row]

    val toUpdate = stampToPending.get(timestamp)
    if (toUpdate == null) {
      logger.error("No pending updates for timestamp " + timestamp)
      return ret
    }

    val it = toUpdate.iterator()
    while(it.hasNext) {
      val pair = it.next()
      val goodItem: Row = getItemByVersion(pair.key, pair.timestamp)
      put_good(pair.key, goodItem)
    }

    stampToPending.remove(timestamp)
    return ret
  }

  private def addItem(key: PrimaryKey, value: Row) {
    var table = dataItems.get(key.table)

    if(table == null) {
      dataItems.putIfAbsent(key.table, new ConcurrentHashMap[KeyTimestampPair, Row](1000000, .2f, 36))
      table = dataItems.get(key.table)
    }

    table.put(new KeyTimestampPair(key, value.timestamp), value)
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

  private[storage] var dataItems = new ConcurrentHashMap[Int, ConcurrentHashMap[KeyTimestampPair, Row]](100, .25f, 36)
  private var latestGoodForKey = new ConcurrentHashMap[Int, ConcurrentHashMap[PrimaryKey, Row]](100, .25f, 36)
  private var stampToPending = new ConcurrentHashMap[Long, List[KeyTimestampPair]](100, .25f, 36)
  private var candidatesForGarbageCollection = new LinkedBlockingQueue[KeyTimestampPair]
  val gcTimeMs = 5000
}


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
