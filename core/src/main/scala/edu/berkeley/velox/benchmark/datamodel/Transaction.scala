package edu.berkeley.velox.benchmark.datamodel

import edu.berkeley.velox.datamodel.{DataItem, ItemKey}
import java.util
import edu.berkeley.velox.storage.StorageEngine
import edu.berkeley.velox.cluster.TPCCPartitioner
import edu.berkeley.velox.rpc.InternalRPCService

import scala.collection.JavaConverters._

import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.benchmark.operation.{GetAllRequest, CommitPutAllRequest, PreparePutAllRequest}
import scala.concurrent.{Promise, Future}
import scala.util.{Failure, Success}
import edu.berkeley.velox.util.NonThreadedExecutionContext._
import edu.berkeley.velox.conf.VeloxConfig
import com.typesafe.scalalogging.slf4j.Logging


class Transaction(val txId: Long, val partitioner: TPCCPartitioner, val storage: StorageEngine, val messageService: InternalRPCService) extends Logging {
  def table(tableName: Int): Table = {
    return new Table(tableName, this)
  }

  def put(key: ItemKey, value: AnyRef): Transaction = {
    if(partitioner.getMasterPartition(key) == VeloxConfig.partitionId)
      toPutLocal.put(key, new DataItem(txId, value))
    else
      toPutRemote.put(key, new DataItem(txId, value))

    return this
  }

  def get(key: ItemKey): Transaction = {
    if(partitioner.getMasterPartition(key) == VeloxConfig.partitionId)
      toGetLocal.add(key)
    else
      toGetRemote.add(key)

    return this
  }

  def executeWriteNonRAMP(engine: StorageEngine) {
    engine.putAll(toPutRemote)
    toPutRemote.clear()
  }

  def executeRead(engine: StorageEngine) {
    results.clear()
    results = engine.getAll(toGetRemote)
    toGetRemote.clear()
  }

  def executeWrite = {

    val p = Promise[Transaction]

    storage.putPending(toPutLocal)

    if(!toPutRemote.isEmpty) {

      val allKeys = new util.ArrayList[ItemKey](toPutRemote.size)

      for(p <- toPutRemote.keySet.asScala) {
        allKeys.add(p)
      }

      toPutRemote.values.asScala.foreach(
       d => d.transactionKeys = allKeys
      )

      val writesByPartition = new util.HashMap[NetworkDestinationHandle, PreparePutAllRequest]

      for(pair: java.util.Map.Entry[ItemKey, DataItem] <- toPutRemote.entrySet.asScala) {
        val partition = partitioner.getMasterPartition(pair.getKey)
        if(!writesByPartition.containsKey(partition)) {
          writesByPartition.put(partition, new PreparePutAllRequest(new util.HashMap[ItemKey, DataItem]))
        }

        writesByPartition.get(partition).values.put(pair.getKey, pair.getValue)
      }

      val prepareFuture = Future.sequence(writesByPartition.asScala.map {
        case (destination, write) => messageService.send(destination, write)
      })

      prepareFuture onComplete {
        case Success(responses) => {
          storage.putGood(toPutLocal.values().iterator().next().timestamp)
          toPutLocal.clear()

          val commitFuture = Future.sequence(writesByPartition.keySet.asScala.map {
            case destination => messageService.send(destination, new CommitPutAllRequest(txId))
          })

          commitFuture onComplete {
            case Success(responses) => p success this
            case Failure(t) => p failure t
          }
        }
        case Failure(t) => {
          p.failure(t)
        }
      }

      toPutRemote.clear()
    } else {
      storage.putGood(toPutLocal.values().iterator().next().timestamp)
      toPutLocal.clear()
      p success this
    }

    p.future
  }

  def executeRead = {

    val p = Promise[Transaction]
    results.clear()

    if(!toGetRemote.isEmpty) {
     val readsByPartition = new util.HashMap[NetworkDestinationHandle, GetAllRequest]

      toGetRemote.asScala.foreach(
        k => {
          val partition = partitioner.getMasterPartition(k)
          if(!readsByPartition.containsKey(partition)) {
            readsByPartition.put(partition, new GetAllRequest(new util.ArrayList[ItemKey]))
          }

          readsByPartition.get(partition).keys.add(k)
        }
      )

     val getFuture = Future.sequence(readsByPartition.asScala.map {
       case (destination, read) => messageService.send(destination, read)
     })

      results.putAll(storage.getAll(toGetLocal))


     getFuture onComplete {
       case Success(responses) => {
         logger.error(s"SUCCESS getFuture!")

         responses foreach {
           r => results.putAll(r.values)
         }

         logger.error(s"post SUCCESS getFuture!")

         p success this
       }
       case Failure(t) => {
         p.failure(t)
       }
     }

     toGetLocal.clear
     toGetRemote.clear
    } else {
      results.putAll(storage.getAll(toGetLocal))
      toGetLocal.clear
      p success this
    }

     p.future
   }

  def getQueryResult(itemKey: ItemKey): Any = {
    return results.get(itemKey).value
  }

  def getRawResult(itemKey: ItemKey): DataItem = {
    return results.get(itemKey)
  }

  private var toPutLocal = new util.HashMap[ItemKey, DataItem]
  private var toGetLocal = new util.ArrayList[ItemKey]

  private var toPutRemote = new util.HashMap[ItemKey, DataItem]
  private var toGetRemote = new util.ArrayList[ItemKey]
  var results = new util.HashMap[ItemKey, DataItem]
}

