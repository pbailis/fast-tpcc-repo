package edu.berkeley.velox.server

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox._
import edu.berkeley.velox.cluster.{TPCCPartitioner}
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.rpc.{FrontendRPCService, InternalRPCService, MessageHandler, Request}
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{Promise, Future, future}
import edu.berkeley.velox.util.NonThreadedExecutionContext._
import edu.berkeley.velox.storage.StorageEngine
import edu.berkeley.velox.benchmark.operation._
import edu.berkeley.benchmark.tpcc.TPCCNewOrder
import edu.berkeley.velox.benchmark.datamodel.serializable.{SerializableRow, LockManager}
import java.util
import edu.berkeley.velox.datamodel.{Row, PrimaryKey}
import edu.berkeley.velox.benchmark.operation.serializable.TPCCNewOrderSerializable
import java.util.Collections


// Every server has a single instance of this class. It handles data edu.berkeley.velox.storage
// and serves client requests. Data is stored in a concurrent hash map.
// As requests come in and are served, the hashmap is accessed concurrently
// by the thread executing the message handlers (and the handlers have a
// reference to the map. For now, the server is oblivious to the keyrange
// it owns. It depends on the routing service to route only the correct
// keys to it.

class VeloxServer extends Logging {
  val storageEngine = new StorageEngine
  storageEngine.initialize
  val partitioner = new TPCCPartitioner
  val lockManager = new LockManager

  val internalServer = new InternalRPCService
  val executor = internalServer.networkService.setExecutor()
  internalServer.initialize()


  internalServer.registerHandler(new InternalGetAllHandler)
  internalServer.registerHandler(new InternalPreparePutAllHandler)
  internalServer.registerHandler(new InternalCommitPutAllHandler)
  internalServer.registerHandler(new InternalSerializableGetAllRequestHandler)
  internalServer.registerHandler(new InternalSerializablePutAllRequestHandler)
  internalServer.registerHandler(new InternalSerializableUnlockRequestHandler)

  // create the message service first, register handlers, then start the network
  val frontendServer = new FrontendRPCService
  frontendServer.networkService.setExecutor(executor)

  frontendServer.registerHandler(new TPCCLoadRequestHandler)
  frontendServer.registerHandler(new TPCCNewOrderRequestHandler)

  frontendServer.initialize()

  /*
   * Handlers for front-end requests.
   */

  class TPCCLoadRequestHandler extends MessageHandler[TPCCLoadResponse, TPCCLoadRequest] {
    def receive(src: NetworkDestinationHandle, msg: TPCCLoadRequest): Future[TPCCLoadResponse] = {
      logger.info(s"Loading TPCC warehouse ${msg.W_ID}")
      val p = Promise[TPCCLoadResponse]
      //hackety hack for multi-warehouse runs
      new Thread(new Runnable {
        override def run() = {
          TPCCLoader.doLoad(msg.W_ID, partitioner, internalServer, storageEngine)
          p.success(new TPCCLoadResponse)
        }
      })
      p.future
    }
  }

  class TPCCNewOrderRequestHandler extends MessageHandler[TPCCNewOrderResponse, TPCCNewOrderRequest] {
    def receive(src: NetworkDestinationHandle, msg: TPCCNewOrderRequest): Future[TPCCNewOrderResponse] = {
      if(!msg.serializable)
        TPCCNewOrder.execute(msg, partitioner, internalServer, storageEngine)
      else
        TPCCNewOrderSerializable.execute(lockManager, msg, partitioner, internalServer, storageEngine)
    }
  }

  /*
   * Handlers for internal routed requests
   */

  // define handlers
  class InternalPreparePutAllHandler extends MessageHandler[PreparePutAllResponse, PreparePutAllRequest] {
    def receive(src: NetworkDestinationHandle, msg: PreparePutAllRequest): Future[PreparePutAllResponse] = {
      future {
        storageEngine.putPending(msg.values)
        new PreparePutAllResponse
      }
    }
  }

  class InternalCommitPutAllHandler extends MessageHandler[CommitPutAllResponse, CommitPutAllRequest] {
    def receive(src: NetworkDestinationHandle, msg: CommitPutAllRequest): Future[CommitPutAllResponse] = {
      future {
        storageEngine.putGood(msg.timestamp, msg.deferredIncrement)
        new CommitPutAllResponse
      }
    }
  }

  class InternalGetAllHandler extends MessageHandler[GetAllResponse, GetAllRequest] {
    def receive(src: NetworkDestinationHandle, msg: GetAllRequest): Future[GetAllResponse] = {
      // returns true if there was an old value
      future { new GetAllResponse(storageEngine.getAll(msg.keys, msg.values)) }
    }
  }

  class InternalSerializableGetAllRequestHandler extends MessageHandler[SerializableGetAllResponse, SerializableGetAllRequest] {
    def receive(src: NetworkDestinationHandle, msg: SerializableGetAllRequest) = {
      future {
        val ret = new util.HashMap[PrimaryKey, Row]
        val keys = new util.ArrayList[PrimaryKey](msg.keys.keySet())
        Collections.sort(keys)
        val get_it = keys.iterator()
        while(get_it.hasNext) {
          val toGet = get_it.next()
          val toGetRow = msg.keys.get(toGet).asInstanceOf[SerializableRow]
          if(toGetRow.forUpdate) {
            lockManager.writeLock(toGet, 8008)
          } else {
            lockManager.readLock(toGet, 8008)
          }

          ret.put(toGet, storageEngine.get(toGet))
        }

        new SerializableGetAllResponse(ret)
      }
    }
  }

  class InternalSerializablePutAllRequestHandler extends MessageHandler[SerializablePutAllResponse, SerializablePutAllRequest] {
     def receive(src: NetworkDestinationHandle, msg: SerializablePutAllRequest) = {
       future {
         val keys = new util.ArrayList[PrimaryKey](msg.values.keySet())
         Collections.sort(keys)

         val put_it = keys.iterator()
         while(put_it.hasNext) {
           val toPut = put_it.next()
           val toPutRow = msg.values.get(toPut).asInstanceOf[SerializableRow]
           if(toPutRow.needsLock) {
             lockManager.writeLock(toPut, 8008)
           }

           storageEngine.put(toPut, toPutRow)
         }

         new SerializablePutAllResponse
       }
     }
   }

  class InternalSerializableUnlockRequestHandler extends MessageHandler[Unit, SerializableUnlockRequest] {
      def receive(src: NetworkDestinationHandle, msg: SerializableUnlockRequest) = {
        future {
          val key_it = msg.keys.iterator()
          while(key_it.hasNext) {
            val key = key_it.next()
            lockManager.unlock(key)
          }
        }
      }
    }
}

object VeloxServer extends Logging {
  def main(args: Array[String]) {
    logger.info("Initializing Server")
    VeloxConfig.initialize(args)
    // initialize network service and message service
    val kvserver = new VeloxServer
  }
}
