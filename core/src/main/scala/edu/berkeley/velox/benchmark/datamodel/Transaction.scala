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



class Transaction(val txId: Long) {
  def table(tableName: Int): Table = {
    return new Table(tableName, this)
  }

  def put(key: ItemKey, value: AnyRef): Transaction = {
    toPut.put(key, new DataItem(txId, value))
    return this
  }

  def get(key: ItemKey): Transaction = {
    toGet.add(key)
    return this
  }

  def executeWriteNonRAMP(engine: StorageEngine) {
    engine.putAll(toPut)
    toPut.clear()
  }

  def executeRead(engine: StorageEngine) {
    results.clear()
    results = engine.getAll(toGet)
    toGet.clear()
  }

  def executeWrite(partitioner: TPCCPartitioner,
                   messageService: InternalRPCService) : Future[Transaction]  = {

    val p = Promise[Transaction]

    val allKeys = new util.ArrayList[ItemKey](toPut.size)

    for(p <- toPut.keySet.asScala) {
      allKeys.add(p)
    }

    toPut.values.asScala.foreach(
     d => d.transactionKeys = allKeys
    )

    val writesByPartition = new util.HashMap[NetworkDestinationHandle, PreparePutAllRequest]

    for(pair: java.util.Map.Entry[ItemKey, DataItem] <- toPut.entrySet.asScala) {
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

    toPut.clear()
    p.future
  }

  def executeRead(partitioner: TPCCPartitioner,
                  messageService: InternalRPCService) : Future[Transaction]  = {

    val p = Promise[Transaction]
    results.clear()


     val readsByPartition = new util.HashMap[NetworkDestinationHandle, GetAllRequest]

      toGet.asScala.foreach(
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

     getFuture onComplete {
       case Success(responses) => {
         responses foreach {
           r => results.putAll(r.values)
         }

         p success this
       }
       case Failure(t) => {
         p.failure(t)
       }
     }

     toPut.clear()
     p.future
   }

  def getQueryResult(itemKey: ItemKey): Any = {
    return results.get(itemKey).value
  }

  def getRawResult(itemKey: ItemKey): DataItem = {
    return results.get(itemKey)
  }

  private var toPut = new util.HashMap[ItemKey, DataItem]
  private var toGet = new util.ArrayList[ItemKey]
  var results = new util.HashMap[ItemKey, DataItem]
}

