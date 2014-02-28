package edu.berkeley.velox.server

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox._
import edu.berkeley.velox.cluster.{TPCCPartitioner}
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.rpc.{FrontendRPCService, InternalRPCService, MessageHandler, Request}
import java.util.concurrent.{Semaphore, Executors, ConcurrentHashMap}
import scala.concurrent.{Promise, Future, future}
import edu.berkeley.velox.storage.StorageEngine
import edu.berkeley.velox.benchmark.operation._
import edu.berkeley.benchmark.tpcc.TPCCNewOrder
  import edu.berkeley.velox.benchmark.datamodel.serializable.{SerializableRow, LockManager}
import java.util
import edu.berkeley.velox.datamodel.{Row, PrimaryKey}
import edu.berkeley.velox.benchmark.operation.serializable.TPCCNewOrderSerializable
import java.util.Collections
import java.util.concurrent.locks.ReentrantLock
import edu.berkeley.velox.benchmark.util.FutureLock
import scala.util.{Failure, Success}

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

  implicit val executionContext = if(!VeloxConfig.serializable) {
         edu.berkeley.velox.util.NonThreadedExecutionContext.context
       } else {
      scala.concurrent.ExecutionContext.Implicits.global
     }

  val internalServer = new InternalRPCService
  val executor = internalServer.networkService.setExecutor()

  internalServer.registerHandler(new InternalGetAllHandler)
  internalServer.registerHandler(new InternalPreparePutAllHandler)
  internalServer.registerHandler(new InternalCommitPutAllHandler)
  internalServer.registerHandler(new InternalSerializableGetAllRequestHandler)
  internalServer.registerHandler(new InternalSerializablePutAllRequestHandler)
  internalServer.registerHandler(new InternalSerializableUnlockRequestHandler)

  internalServer.registerHandler(new MicrobenchmarkTwoPLUnlockHandler)
  internalServer.registerHandler(new MicrobenchmarkOptimizedTwoPLHandler)

  internalServer.initialize()

  // create the message service first, register handlers, then start the network
  val frontendServer = new FrontendRPCService
  frontendServer.networkService.setExecutor(executor)

  frontendServer.registerHandler(new TPCCLoadRequestHandler)
  frontendServer.registerHandler(new TPCCNewOrderRequestHandler)

  frontendServer.registerHandler(new MicrobenchmarkTwoPLUnlockHandler)
  frontendServer.registerHandler(new MicrobenchmarkOptimizedTwoPLHandler)

  frontendServer.registerHandler(new MicrobenchmarkCfreePutHandler)
  frontendServer.registerHandler(new MicrobenchmarkTwoPLPutAndLockHandler)


  frontendServer.initialize()

  /*
   * Handlers for front-end requests.
   */

  class TPCCLoadRequestHandler extends MessageHandler[TPCCLoadResponse, TPCCLoadRequest] {
    def receive(src: NetworkDestinationHandle, requestID: RequestId, msg: TPCCLoadRequest): Future[TPCCLoadResponse] = {
      logger.info(s"Loading TPCC warehouse ${msg.W_ID}")
      TPCCLoader.doLoad(msg.W_ID, partitioner, internalServer, storageEngine)
      future {
        new TPCCLoadResponse
      }
    }
  }

  class TPCCNewOrderRequestHandler extends MessageHandler[TPCCNewOrderResponse, TPCCNewOrderRequest] {
    def receive(src: NetworkDestinationHandle, requestID: RequestId, msg: TPCCNewOrderRequest): Future[TPCCNewOrderResponse] = {
      if (!msg.serializable)
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
    def receive(src: NetworkDestinationHandle, requestID: RequestId, msg: PreparePutAllRequest): Future[PreparePutAllResponse] = {
      future {
        storageEngine.putPending(msg.values)
        new PreparePutAllResponse
      }
    }
  }

  class InternalCommitPutAllHandler extends MessageHandler[CommitPutAllResponse, CommitPutAllRequest] {
    def receive(src: NetworkDestinationHandle, requestID: RequestId, msg: CommitPutAllRequest): Future[CommitPutAllResponse] = {
      future {
        storageEngine.putGood(msg.timestamp, msg.deferredIncrement)
        new CommitPutAllResponse
      }
    }
  }

  class InternalGetAllHandler extends MessageHandler[GetAllResponse, GetAllRequest] {
    def receive(src: NetworkDestinationHandle, requestID: RequestId, msg: GetAllRequest): Future[GetAllResponse] = {
      // returns true if there was an old value
      future {
        new GetAllResponse(storageEngine.getAll(msg.keys, msg.values))
      }
    }
  }

  class InternalSerializableGetAllRequestHandler extends MessageHandler[SerializableGetAllResponse, SerializableGetAllRequest] {
    def receive(src: NetworkDestinationHandle, requestID: RequestId, msg: SerializableGetAllRequest) = {
      future {
        val ret = new util.HashMap[PrimaryKey, Row]
        val keys = new util.ArrayList[PrimaryKey](msg.keys.keySet())
        Collections.sort(keys)
        val get_it = keys.iterator()
        while (get_it.hasNext) {
          val toGet = get_it.next()
          val toGetRow = msg.keys.get(toGet).asInstanceOf[SerializableRow]
          if (toGetRow.forUpdate) {
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
    def receive(src: NetworkDestinationHandle, requestID: RequestId, msg: SerializablePutAllRequest) = {
      future {
        val keys = new util.ArrayList[PrimaryKey](msg.values.keySet())
        Collections.sort(keys)

        val put_it = keys.iterator()
        while (put_it.hasNext) {
          val toPut = put_it.next()
          val toPutRow = msg.values.get(toPut).asInstanceOf[SerializableRow]
          if (toPutRow.needsLock) {
            lockManager.writeLock(toPut, 8008)
          }

          storageEngine.put(toPut, toPutRow)
        }

        new SerializablePutAllResponse
      }
    }
  }

  class InternalSerializableUnlockRequestHandler extends MessageHandler[Unit, SerializableUnlockRequest] {
    def receive(src: NetworkDestinationHandle, requestID: RequestId, msg: SerializableUnlockRequest) = {
      future {
        val key_it = msg.keys.iterator()
        while (key_it.hasNext) {
          val key = key_it.next()
          lockManager.unlock(key)
        }
      }
    }
  }

  val microbenchmarkLock = new Semaphore(1)
  var microbenchmarkItem = 0L
  // handlers for microbenchmark code

  class MicrobenchmarkCfreePutHandler extends MessageHandler[Boolean, MicroCfreePut] {
    def receive(src: NetworkDestinationHandle, requestID: RequestId, msg: MicroCfreePut) = {
      future {
        microbenchmarkItem += 1
        true
      }
    }
  }

  class MicrobenchmarkTwoPLPutAndLockHandler extends MessageHandler[Boolean, MicroTwoPLPutAndLock] {
    def receive(src: NetworkDestinationHandle, requestID: RequestId, msg: MicroTwoPLPutAndLock) = {
      future {
        microbenchmarkLock.acquireUninterruptibly()
        microbenchmarkItem += 1
        true
      }
    }
  }

  class MicrobenchmarkTwoPLUnlockHandler extends MessageHandler[Unit, MicroTwoPLUnlock] {
    def receive(src: NetworkDestinationHandle, requestID: RequestId, msg: MicroTwoPLUnlock) = {
      microbenchmarkLock.release()
      future {}
    }
  }

  class MicrobenchmarkOptimizedTwoPLHandler extends MessageHandler[Boolean, MicroOptimizedTwoPL] {
    def receive(src: NetworkDestinationHandle, requestID: RequestId, msg: MicroOptimizedTwoPL): Future[Boolean] = {
      var p = Promise[Boolean]

        microbenchmarkLock.acquireUninterruptibly()
        microbenchmarkItem += 1

        if (msg.numItems == 1) {
          microbenchmarkLock.release()
          p.success(true)
        } else if (msg.numRemaining == 1) {
          microbenchmarkLock.release()
          var prev_index = 1
          // unlock all previous senders!
          while (prev_index < msg.numItems) {
            var server_id = VeloxConfig.partitionId - prev_index
            if (server_id < 0) {
              server_id = VeloxConfig.partitionList.size - 1 + server_id
            }
            internalServer.send(server_id, new MicroTwoPLUnlock)
            prev_index += 1
          }

          frontendServer.sendResponse(msg.clientID, msg.clientRequestID, true)
          p.success(true)
        } else {
          val nextServer = (VeloxConfig.partitionId + 1) % VeloxConfig.partitionList.size
          var clientRequest = msg.clientRequestID
          if (clientRequest == -1) {
            clientRequest = requestID
          }

          internalServer.send(nextServer, new MicroOptimizedTwoPL(msg.clientID, clientRequest, msg.numItems, msg.numRemaining - 1))
          p = null
        }

      if(p == null) {
        null
      } else {
        p.future
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

