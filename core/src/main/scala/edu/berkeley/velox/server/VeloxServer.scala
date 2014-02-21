package edu.berkeley.velox.server

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox._
import edu.berkeley.velox.cluster.{TPCCPartitioner}
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.rpc.{FrontendRPCService, InternalRPCService, MessageHandler, Request}
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{Future, future}
import edu.berkeley.velox.util.NonThreadedExecutionContext._
import edu.berkeley.velox.storage.StorageEngine
import edu.berkeley.velox.benchmark.operation._
import edu.berkeley.benchmark.tpcc.TPCCNewOrder


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

  val internalServer = new InternalRPCService
  internalServer.initialize()

  internalServer.registerHandler(new InternalGetAllHandler)
  internalServer.registerHandler(new InternalPreparePutAllHandler)
  internalServer.registerHandler(new InternalCommitPutAllHandler)

  // create the message service first, register handlers, then start the network
  val frontendServer = new FrontendRPCService

  frontendServer.registerHandler(new TPCCLoadRequestHandler)
  frontendServer.registerHandler(new TPCCNewOrderRequestHandler)

  frontendServer.initialize()

  /*
   * Handlers for front-end requests.
   */

  class TPCCLoadRequestHandler extends MessageHandler[TPCCLoadResponse, TPCCLoadRequest] {
    def receive(src: NetworkDestinationHandle, msg: TPCCLoadRequest): Future[TPCCLoadResponse] = {
      logger.info(s"Loading TPCC warehouse ${msg.W_ID}")
      TPCCLoader.doLoad(msg.W_ID, partitioner, internalServer, storageEngine)
      future { new TPCCLoadResponse }
    }
  }

  class TPCCNewOrderRequestHandler extends MessageHandler[TPCCNewOrderResponse, TPCCNewOrderRequest] {
    def receive(src: NetworkDestinationHandle, msg: TPCCNewOrderRequest): Future[TPCCNewOrderResponse] = {
      TPCCNewOrder.execute(msg, partitioner, internalServer, storageEngine)
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
      future { new GetAllResponse(storageEngine.getAll(msg.keys)) }
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
