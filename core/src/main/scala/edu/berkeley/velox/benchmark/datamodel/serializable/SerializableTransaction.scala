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
import java.util.Collections

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
    value.timestamp = txId
    if(partitioner.getMasterPartition(key) == VeloxConfig.partitionId) {
      toPutLocal.put(key, value)
      //logger.error(s"$txId local put $key $value")
    }
    else {
      toPutRemote.put(key, value)
      //logger.error(s"$txId remote put $key $value")

    }

    return this
  }

  override def get(key: PrimaryKey, columns: Row): SerializableTransaction = {
    if(partitioner.getMasterPartition(key) == VeloxConfig.partitionId) {
      ///logger.error(s"$txId local get $key $columns $toGetLocal")
      val prev = toGetLocal.put(key, columns)
      //if(prev != null) {
      //  logger.error(s"whoa--just replaced key $key (value: ${prev.columns}; new value: ${columns.columns}")
      //}
      //logger.error(s"$txId local post-put get $key $columns $toGetLocal ${toGetLocal.size}")
    }
    else {
      //logger.error(s"$txId remote get $key $columns")
      toGetRemote.put(key, columns)
    }

    return this
  }

  override def executeRead(engine: StorageEngine) {
    results.clear()
    results = engine.getAll(toGetRemote)
    toGetRemote.clear()
  }

  override def executeWriteLocal {
    storage.putAll(toPutLocal)
    toPutLocal.clear()
  }

  override def executeWrite = {
    val p = Promise[SerializableTransaction]

    //logger.error(s"$txId is going to writelock ${toPutLocal.keySet}")
    val tpl_it = toPutLocal.entrySet().iterator()
    while(tpl_it.hasNext) {
      val toPut = tpl_it.next()

      if(!writeLocked.contains(toPut.getKey)) {
        //logger.error(s"$txId is writelocking ${toPut.getKey}")
        lockTable.writeLock(toPut.getKey)
        //logger.error(s"$txId locked ${toPut.getKey}")

      }
      storage.put(toPut.getKey, toPut.getValue)
    }

    if(!toPutRemote.isEmpty) {
      val writesByPartition = new util.HashMap[NetworkDestinationHandle, SerializablePutAllRequest]

      val tpr_entry_it = toPutRemote.entrySet().iterator()

      while(tpr_entry_it.hasNext) {
        val pair = tpr_entry_it.next
        val partition = partitioner.getMasterPartition(pair.getKey)
        if(!writesByPartition.containsKey(partition)) {
          writesByPartition.put(partition, new SerializablePutAllRequest(new util.HashMap[PrimaryKey, Row]))
        }

        writesByPartition.get(partition).values.put(pair.getKey, pair.getValue)

        if(writeLocked.contains(pair.getKey)) {
          pair.getValue.asInstanceOf[SerializableRow].needsLock = false
        }
      }

      val prepareFutures = new util.ArrayList[Future[Any]](writesByPartition.size())

      val wbp_it = writesByPartition.entrySet().iterator()
      while(wbp_it.hasNext) {
        val wbp = wbp_it.next()
        prepareFutures.add(messageService.send(wbp.getKey, wbp.getValue))
      }

      val prepareFuture = Future.sequence(prepareFutures.asScala)

      prepareFuture onComplete {
        case Success(responses) => {
          p success this
        }
      case Failure(t) => p failure t
        }
    } else {
      p success this
    }
    p.future
  }

  override def executeRead = {

    //logger.error(s"$txId is going to readlock ${toGetLocal.keySet}")


    val p = Promise[SerializableTransaction]
    results.clear()

    val tgl_it = toGetLocal.entrySet().iterator()
    while(tgl_it.hasNext) {
      val toGet = tgl_it.next()
      val toGetRow = toGet.getValue.asInstanceOf[SerializableRow]
      if(toGetRow.forUpdate) {
        //logger.error(s"$txId is going to lock for update ${toGet.getKey}")
        lockTable.writeLock(toGet.getKey)
        writeLocked.add(toGet.getKey)
        //logger.error(s"$txId locked for updated ${toGet.getKey}")
      } else {
        //logger.error(s"$txId is going to read lock ${toGet.getKey}")

        lockTable.readLock(toGet.getKey)
      }
    }

    results.putAll(storage.getAll(toGetLocal))

    if(!toGetRemote.isEmpty) {
     val readsByPartition = new util.HashMap[NetworkDestinationHandle, GetAllRequest]

      val tgr_it = toGetRemote.entrySet().iterator()
      while(tgr_it.hasNext) {
        val entry = tgr_it.next()
        val partition = partitioner.getMasterPartition(entry.getKey)
        if(!readsByPartition.containsKey(partition)) {
          readsByPartition.put(partition, new GetAllRequest(new util.HashMap[PrimaryKey, Row]))
        }

        readsByPartition.get(partition).keys.put(entry.getKey, entry.getValue)
      }

      val getFutures = new util.ArrayList[Future[GetAllResponse]](readsByPartition.size())

      val rbp_it = readsByPartition.entrySet().iterator()
      while(rbp_it.hasNext) {
        val rbp = rbp_it.next()
        getFutures.add(messageService.send(rbp.getKey, rbp.getValue))
      }

     val getFuture = Future.sequence(getFutures.asScala)

     getFuture onComplete {
       case Success(responses) => {

         val resp_it = responses.iterator
         while(resp_it.hasNext) {
           results.putAll(resp_it.next().values)
         }

         p success this
       }
       case Failure(t) => {
         p.failure(t)
       }
     }
    } else {
      p success this
    }

     p.future
   }

  def commit() {
    cleanup(true)
  }

  def abort() {
    cleanup(false)
  }

  private def cleanup(cleanupWrites: Boolean) {
    val local_keys = new util.HashSet[PrimaryKey]
    local_keys.addAll(toGetLocal.keySet())

    if(cleanupWrites)
      local_keys.addAll(toPutLocal.keySet())

    val local_it = local_keys.iterator()

    while(local_it.hasNext) {
      lockTable.unlock(local_it.next())
    }

    if(!toPutRemote.isEmpty || !toGetRemote.isEmpty) {
      val unlockByPartition = new util.HashMap[NetworkDestinationHandle, SerializableUnlockRequest]

      val tgr_it = toGetRemote.keySet().iterator()
      while(tgr_it.hasNext) {
        val entry = tgr_it.next()
        val partition = partitioner.getMasterPartition(entry)
        if(!unlockByPartition.containsKey(partition)) {
          unlockByPartition.put(partition, new SerializableUnlockRequest(new util.HashSet[PrimaryKey]))
        }

        unlockByPartition.get(partition).keys.add(entry)
      }

      if(cleanupWrites) {
        val tpr_it = toPutRemote.entrySet.iterator()
        while(tpr_it.hasNext) {
          val entry = tpr_it.next()

          if(entry.getValue.asInstanceOf[SerializableRow].needsLock) {

            val partition = partitioner.getMasterPartition(entry.getKey)
            if(!unlockByPartition.containsKey(partition)) {
              unlockByPartition.put(partition, new SerializableUnlockRequest(new util.HashSet[PrimaryKey]))
            }

            unlockByPartition.get(partition).keys.add(entry.getKey)
          }
        }
      }

      val unlockFutures = new util.ArrayList[Future[SerializableUnlockResponse]](unlockByPartition.size())

      val rbp_it = unlockByPartition.entrySet().iterator()
      while(rbp_it.hasNext) {
        val rbp = rbp_it.next()
        unlockFutures.add(messageService.send(rbp.getKey, rbp.getValue))
      }

     val unlockFuture = Future.sequence(unlockFutures.asScala)
     Await.ready(unlockFuture, Duration.Inf)

    }

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

  private var toPutLocal = new util.TreeMap[PrimaryKey, Row]
  private var toGetLocal = new util.TreeMap[PrimaryKey, Row]

  private var toPutRemote = new util.TreeMap[PrimaryKey, Row]
  private var toGetRemote = new util.TreeMap[PrimaryKey, Row]
}

