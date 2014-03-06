package edu.berkeley.velox.server

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox._
import edu.berkeley.velox.cluster.{TPCCPartitioner}
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.rpc.{FrontendRPCService, InternalRPCService, MessageHandler, Request}
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{Future, future}
import edu.berkeley.velox.storage.StorageEngine
import edu.berkeley.velox.benchmark.operation._
import edu.berkeley.benchmark.tpcc.TPCCNewOrder
import edu.berkeley.velox.benchmark.datamodel.serializable.{SerializableRow, LockManager}
import java.util
import edu.berkeley.velox.datamodel.{Row, PrimaryKey}
import edu.berkeley.velox.benchmark.operation.serializable.TPCCNewOrderSerializable
import java.util.Collections

import edu.berkeley.velox.util.NonThreadedExecutionContext.context

// Every server has a single instance of this class. It handles data edu.berkeley.velox.storage
// and serves client requests. Data is stored in a concurrent hash map.
// As requests come in and are served, the hashmap is accessed concurrently
// by the thread executing the message handlers (and the handlers have a
// reference to the map. For now, the server is oblivious to the keyrange
// it owns. It depends on the routing service to route only the correct
// keys to it.

class VeloxServer extends Logging {
  // create the message service first, register handlers, then start the network
  val frontendServer = new FrontendRPCService
  frontendServer.networkService.setExecutor()

  frontendServer.registerHandler(new SequenceNumberHandler)

  frontendServer.initialize()

  /*
   * Handlers for front-end requests.
   */


  class SequenceNumberHandler extends MessageHandler[Long, SequenceNumberReq] {
    def receive(src: NetworkDestinationHandle, msg: SequenceNumberReq): Future[Long] = {
      future {
        msg.reqId
      }
    }
  }
}

object VeloxServer extends Logging {
  def main(args: Array[String]) {
    logger.info("Initializing Server")
    VeloxConfig.initialize(args)
    // initialize network service and message service
    new VeloxServer
  }
}

class SequenceNumberReq(val reqId: Long) extends Request[Long]
