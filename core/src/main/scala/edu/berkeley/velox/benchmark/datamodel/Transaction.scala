package edu.berkeley.velox.benchmark.datamodel

import edu.berkeley.velox.datamodel.{PrimaryKey, Row}
import java.util
import edu.berkeley.velox.storage.StorageEngine
import edu.berkeley.velox.cluster.TPCCPartitioner
import edu.berkeley.velox.rpc.InternalRPCService

import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.benchmark.operation._
import scala.concurrent.{Promise, Future}
import scala.util.{Failure, Success}
import edu.berkeley.velox.util.NonThreadedExecutionContext._
import edu.berkeley.velox.conf.VeloxConfig
import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.JavaConverters._
import edu.berkeley.velox.benchmark.TPCCConstants
import scala.util.Failure
import edu.berkeley.velox.benchmark.operation.GetAllRequest
import edu.berkeley.velox.benchmark.operation.PreparePutAllRequest
import scala.util.Success
import edu.berkeley.velox.benchmark.operation.CommitPutAllRequest
import edu.berkeley.velox.benchmark.operation.DeferredIncrement


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

  def executeWriteLocal {
    storage.putAll(toPutLocal)
    toPutLocal.clear()
  }

  def executeWrite = {

    val p = Promise[Transaction]

    val allKeys = new util.ArrayList[PrimaryKey](toPutRemote.size+toPutLocal.size)

    val tpr_key_it = toPutRemote.keySet().iterator()
    while(tpr_key_it.hasNext) {
      allKeys.add(tpr_key_it.next())
    }


    val tpl_key_it = toPutLocal.keySet().iterator()
    while(tpl_key_it.hasNext) {
      allKeys.add(tpl_key_it.next())
    }

    val keyArr: Array[PrimaryKey] = allKeys.toArray(new Array[PrimaryKey](allKeys.size()))

    val tpl_val_it = toPutLocal.values().iterator()
    while(tpl_val_it.hasNext) {
      tpl_val_it.next().transactionKeys = keyArr
    }

    storage.putPending(toPutLocal)

    if(!toPutRemote.isEmpty) {

      val tpr_val_it = toPutRemote.values().iterator()
      while(tpr_val_it.hasNext) {
        tpr_val_it.next().transactionKeys = keyArr
      }

      val writesByPartition = new util.HashMap[NetworkDestinationHandle, PreparePutAllRequest]

      val tpr_entry_it = toPutRemote.entrySet().iterator()

      while(tpr_entry_it.hasNext) {
        val pair = tpr_entry_it.next
        val partition = partitioner.getMasterPartition(pair.getKey)
        if(!writesByPartition.containsKey(partition)) {
          writesByPartition.put(partition, new PreparePutAllRequest(new util.HashMap[PrimaryKey, Row]))
        }

        writesByPartition.get(partition).values.put(pair.getKey, pair.getValue)
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
          deferredIncrementResponse = storage.putGood(txId, deferredIncrement)
          toPutLocal.clear()

          var deferredPartition = -1
          if(deferredIncrement != null) {
            deferredPartition = partitioner.getMasterPartition(deferredIncrement.counterKey)
          }

          val commitFutures = new util.ArrayList[Future[CommitPutAllResponse]](writesByPartition.size())

          val cf_it = writesByPartition.keySet().iterator()
          while(cf_it.hasNext) {
            var deferral: DeferredIncrement = null

            val destination = cf_it.next()

            if(destination == deferredPartition) {
              deferral = deferredIncrement
            }

            messageService.send(destination, new CommitPutAllRequest(txId, deferral))
          }

          val commitFuture = Future.sequence(commitFutures.asScala)

          commitFuture onComplete {
            case Success(responses) => {
              val resp_it = responses.iterator
              while(resp_it.hasNext && deferredIncrementResponse == -1) {
                val response = resp_it.next()
                if(response.incrementResponse > -1) {
                  deferredIncrementResponse = response.incrementResponse
                }
              }
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
      deferredIncrementResponse = storage.putGood(txId, deferredIncrement)
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

      results.putAll(storage.getAll(toGetLocal))


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

  def setDeferredIncrement(d: DeferredIncrement): Transaction = {
    deferredIncrement = d
    this
  }

  var deferredIncrementResponse = -1
  private var deferredIncrement: DeferredIncrement = null

  private var toPutLocal = new util.HashMap[PrimaryKey, Row]
  private var toGetLocal = new util.HashMap[PrimaryKey, Row]

  private var toPutRemote = new util.HashMap[PrimaryKey, Row]
  private var toGetRemote = new util.HashMap[PrimaryKey, Row]
  var results = new util.HashMap[PrimaryKey, Row]

  def combineFuture[T](futures: util.ArrayList[Future[T]]): Future[util.Vector[T]] = {
    val p = Promise[util.Vector[T]]
    val totalFutures = futures.size()
    val ret = new util.Vector[T](totalFutures)
    val future_it = futures.iterator()
    while(future_it.hasNext()) {
      val future = future_it.next()
      future.onComplete {
        case Success(r) => {
          ret.add(r)
          if(ret.size() == totalFutures) {
            logger.error(s"ALL got ${ret.size()} of $totalFutures")
            p.trySuccess(ret)
          } else {
            logger.error(s"NOT got ${ret.size()} of $totalFutures")
          }
        }
        case Failure(t) => p tryFailure t
      }
    }
    p.future
  }
}

