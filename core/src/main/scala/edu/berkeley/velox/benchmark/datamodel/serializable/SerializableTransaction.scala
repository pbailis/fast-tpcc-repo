package edu.berkeley.velox.benchmark.datamodel.serializable

import edu.berkeley.velox.datamodel.{PrimaryKey, Row}
import java.util
import edu.berkeley.velox.storage.StorageEngine
import edu.berkeley.velox.cluster.TPCCPartitioner
import edu.berkeley.velox.rpc.InternalRPCService

import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.benchmark.operation._
import scala.concurrent.{Await, Promise, Future}
import edu.berkeley.velox.util.NonThreadedExecutionContext._
import edu.berkeley.velox.conf.VeloxConfig
import scala.collection.JavaConverters._
import scala.util.{Random, Failure, Success}
import edu.berkeley.velox.benchmark.operation.GetAllRequest
import edu.berkeley.velox.benchmark.operation.PreparePutAllRequest
import edu.berkeley.velox.benchmark.operation.CommitPutAllRequest
import edu.berkeley.velox.benchmark.operation.DeferredIncrement
import edu.berkeley.velox.benchmark.datamodel.{Table, Transaction}
import scala.concurrent.duration.Duration
import java.util.{Comparator, Collections}
import edu.berkeley.velox.benchmark.{TPCCItemKey, TPCCConstants}

object SerializableTransaction {
  val FOR_UPDATE = true
}

class SerializableTransaction(lockTable: LockManager,
                              txId: Long,
                              partitioner: TPCCPartitioner,
                              storage: StorageEngine,
                              messageService: InternalRPCService)
  extends Transaction(txId, partitioner, storage, messageService) {

  override def table(tableName: Int): Table = {
    return new Table(tableName, this)
  }

  override def put(key: PrimaryKey, value: Row): SerializableTransaction = {
    writeKeys.add(key)

    if(value.isInstanceOf[SerializableRow] && writeLocked.contains(key)) {
      value.asInstanceOf[SerializableRow].needsLock = false
    }

    value.timestamp = txId
    var partition = VeloxConfig.partitionId
    if(key.table != TPCCConstants.ITEM_TABLE) {
      partition = partitioner.getMasterPartition(key)
    }

    var keys = toPut.get(partition)
    if(keys == null) {
      keys = new util.HashMap[PrimaryKey, Row]
      toPut.put(partition, keys)
    }

    toPut.get(partition).put(key, value)

    return this
  }

  override def get(key: PrimaryKey, columns: Row): SerializableTransaction = {
    readKeys.add(key)

    if(columns.isInstanceOf[SerializableRow] && columns.asInstanceOf[SerializableRow].forUpdate) {
      writeLocked.add(key)
    }


    var partition = VeloxConfig.partitionId
    if(key.table != TPCCConstants.ITEM_TABLE) {
      partition = partitioner.getMasterPartition(key)
    }

    var keys = toGet.get(partition)
    if(keys == null) {
      keys = new util.HashMap[PrimaryKey, Row]
      toGet.put(partition, keys)
    }

    toGet.get(partition).put(key, columns)

    return this
  }



  override def executeWrite = {
    val p = Promise[SerializableTransaction]

    logger.error(s"locking!")

    val partitions = new util.ArrayList[Int](toPut.keySet)
    Collections.sort(partitions, IntComparator)

    val p_it = partitions.iterator()
    while(p_it.hasNext) {
      val partition = p_it.next()

      if(partition == VeloxConfig.partitionId) {
        val keys = new util.ArrayList[PrimaryKey](toPut.get(partition).keySet())
        Collections.sort(keys)
        var key_it = keys.iterator()
        while(key_it.hasNext) {
          val lockKey = key_it.next()
          if(!writeLocked.contains(lockKey)) {
            lockTable.writeLock(lockKey,txId)
          }
        }
        storage.putAll(toPut.get(partition))

      } else {
        val f = messageService.send(partition, new SerializablePutAllRequest(toPut.get(partition)))
        Await.ready(f, Duration.Inf)
      }
    }

    logger.error(s"locked!")


    p success this
    p.future
  }

  override def executeRead = {
    val p = Promise[SerializableTransaction]


    logger.error(s"locking!")

    val partitions = new util.ArrayList[Int](toGet.keySet)
    Collections.sort(partitions, IntComparator)

    val p_it = partitions.iterator()
    while(p_it.hasNext) {
      val partition = p_it.next()

      if(partition == VeloxConfig.partitionId) {
        val partitionEntries = toGet.get(partition)

        //logger.error(s"$txId locking $partitionEntries")

        val keys = new util.ArrayList[PrimaryKey](partitionEntries.keySet())
        Collections.sort(keys)
        val key_it = keys.iterator()
        while(key_it.hasNext) {
          val key = key_it.next()
          val forUpdate = partitionEntries.get(key).asInstanceOf[SerializableRow].forUpdate
          if(forUpdate) {
            lockTable.writeLock(key, txId)
          } else {
            lockTable.readLock(key, txId)
          }
        }
        results.putAll(storage.getAll(toGet.get(partition)))
      } else {

        //logger.error(s"$txId locking ${toGet.get(warehouse)}")

        val f = messageService.send(partition, new SerializableGetAllRequest(toGet.get(partition)))
        Await.ready(f, Duration.Inf)
        results.putAll(f.value.get.get.values)
      }
    }

    //logger.error(s"$txId finished locking")

    logger.error(s"locked!")


    p success this

     p.future
   }

  def commit() {
    cleanup(true)
  }

  def abort() {
    cleanup(false)
  }

  private def cleanup(cleanupWrites: Boolean) {
    var key_it = readKeys.iterator()
    val partitionToUnlock = new util.HashMap[NetworkDestinationHandle, util.HashSet[PrimaryKey]]()


    logger.error(s"unlocking!")

  while(key_it.hasNext) {
    val key = key_it.next()
    val partition = partitioner.getMasterPartition(key)

    var partitionSet = partitionToUnlock.get(partition)
    if(partitionSet == null) {
      partitionSet = new util.HashSet[PrimaryKey]()
      partitionToUnlock.put(partition, partitionSet)
    }

    partitionSet.add(key)
  }

    if(cleanupWrites) {

      key_it = writeKeys.iterator()

      while(key_it.hasNext) {
        val key = key_it.next()
        val partition = partitioner.getMasterPartition(key)

        var partitionSet = partitionToUnlock.get(partition)
        if(partitionSet == null) {
          partitionSet = new util.HashSet[PrimaryKey]()
          partitionToUnlock.put(partition, partitionSet)
        }

        partitionSet.add(key)
      }
    }

  val dest_it = partitionToUnlock.keySet.iterator()
  while(dest_it.hasNext) {
    val dest = dest_it.next()


    if(dest == VeloxConfig.partitionId) {
      val key_it = partitionToUnlock.get(dest).iterator()
      while(key_it.hasNext) {
        val key = key_it.next()
        lockTable.unlock(key)
      }
    } else {
      messageService.send(dest, new SerializableUnlockRequest(partitionToUnlock.get(dest)))
    }


    logger.error(s"unlocked!")

  }

  override def getQueryResult(itemKey: PrimaryKey, column: Int): Any = {
    if(!results.containsKey(itemKey)) {
      logger.error(s"$txId wanted $itemKey but only have $results")
    }

    return results.get(itemKey).readColumn(column)
  }

  override def getRawResult(itemKey: PrimaryKey): Row = {
    return results.get(itemKey)
  }

  override def setDeferredIncrement(d: DeferredIncrement): SerializableTransaction = {
    deferredIncrement = d
    this
  }


  private var deferredIncrement: DeferredIncrement = null


  private var writeLocked = new util.HashSet[PrimaryKey]

  private var toPut = new util.HashMap[Int, util.HashMap[PrimaryKey, Row]]
  private var toGet = new util.HashMap[Int, util.HashMap[PrimaryKey, Row]]
  private var writeKeys = new util.HashSet[PrimaryKey]
  private var readKeys = new util.HashSet[PrimaryKey]
}

object IntComparator extends Comparator[Int] {
  override def compare(a: Int, b: Int): Int = {
    return a.compareTo(b)
  }
}
