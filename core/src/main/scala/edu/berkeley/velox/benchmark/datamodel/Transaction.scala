package edu.berkeley.velox.benchmark.datamodel

import edu.berkeley.velox.datamodel.{PrimaryKey, Row}
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
import edu.berkeley.velox.benchmark.TPCCConstants


class Transaction(val txId: Long, val partitioner: TPCCPartitioner, val storage: StorageEngine, val messageService: InternalRPCService) extends Logging {
  def table(tableName: Int): Table = {
    return new Table(tableName, this)
  }

  def put(key: PrimaryKey, value: Row): Transaction = {
    value.timestamp = txId
    if(partitioner.getMasterPartition(key) == VeloxConfig.partitionId)
      toPutLocal.put(key, value)
    else
      toPutRemote.put(key, value)

    return this
  }

  def get(key: PrimaryKey, columns: Row): Transaction = {
    if(partitioner.getMasterPartition(key) == VeloxConfig.partitionId)
      toGetLocal.put(key, columns)
    else
      toGetRemote.put(key, columns)

    return this
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

      val allKeys = new util.ArrayList[PrimaryKey](toPutRemote.size)

      for(p <- toPutRemote.keySet.asScala) {
        allKeys.add(p)
      }

      toPutRemote.values.asScala.foreach(
       d => d.transactionKeys = allKeys
      )

      val writesByPartition = new util.HashMap[NetworkDestinationHandle, PreparePutAllRequest]

      for(pair: java.util.Map.Entry[PrimaryKey, Row] <- toPutRemote.entrySet.asScala) {
        val partition = partitioner.getMasterPartition(pair.getKey)
        if(!writesByPartition.containsKey(partition)) {
          writesByPartition.put(partition, new PreparePutAllRequest(new util.HashMap[PrimaryKey, Row]))
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
            case Success(responses) => {
              p success this
            }
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
          val partition = partitioner.getMasterPartition(k._1)
          if(!readsByPartition.containsKey(partition)) {
            readsByPartition.put(partition, new GetAllRequest(new util.HashMap[PrimaryKey, Row]))
          }

          readsByPartition.get(partition).keys.put(k._1, k._2)
        }
      )

     val getFuture = Future.sequence(readsByPartition.asScala.map {
       case (destination, read) => messageService.send(destination, read)
     })

      results.putAll(storage.getAll(toGetLocal))


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

     toGetLocal.clear
     toGetRemote.clear
    } else {
      results.putAll(storage.getAll(toGetLocal))
      toGetLocal.clear
      p success this
    }

     p.future
   }

  def getQueryResult(itemKey: PrimaryKey, column: Int): Any = {
    return results.get(itemKey).readColumn(column)
  }

  def getRawResult(itemKey: PrimaryKey): Row = {
    return results.get(itemKey)
  }

  private var toPutLocal = new util.HashMap[PrimaryKey, Row]
  private var toGetLocal = new util.HashMap[PrimaryKey, Row]

  private var toPutRemote = new util.HashMap[PrimaryKey, Row]
  private var toGetRemote = new util.HashMap[PrimaryKey, Row]
  var results = new util.HashMap[PrimaryKey, Row]
}

