package edu.berkeley.velox.server

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox._
import edu.berkeley.velox.cluster.RandomPartitioner
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.datamodel.{Key, Value}
import edu.berkeley.velox.rpc.{FrontendRPCService, InternalRPCService, MessageHandler, Request}
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{Future, future}
import scala.concurrent.ExecutionContext.Implicits.global


// Every server has a single instance of this class. It handles data storage
// and serves client requests. Data is stored in a concurrent hash map.
// As requests come in and are served, the hashmap is accessed concurrently
// by the thread executing the message handlers (and the handlers have a
// reference to the map. For now, the server is oblivious to the keyrange
// it owns. It depends on the routing service to route only the correct
// keys to it.

class VeloxServer extends Logging {
  val datastore = new ConcurrentHashMap[Key, Value]()
  val partitioner = new RandomPartitioner

  val internalServer = new InternalRPCService
  internalServer.initialize()

  internalServer.registerHandler(new InternalPutHandler)
  internalServer.registerHandler(new InternalGetHandler)
  internalServer.registerHandler(new InternalInsertHandler)

  // create the message service first, register handlers, then start the network
  val frontendServer = new FrontendRPCService

  frontendServer.registerHandler(new FrontendPutRequestHandler)
  frontendServer.registerHandler(new FrontendPutRequestHandler)
  frontendServer.registerHandler(new FrontendGetRequestHandler)

  frontendServer.initialize()

  /*
   * Handlers for front-end requests.
   */

  class FrontendPutRequestHandler extends MessageHandler[Value, ClientPutRequest] {
    def receive(src: NetworkDestinationHandle, msg: ClientPutRequest): Future[Value] = {
      internalServer.send(partitioner.getMasterPartition(msg.k), RoutedPutRequest(msg.k, msg.v))
    }
  }

  class FrontendInsertRequestHandler extends MessageHandler[Boolean, ClientInsertRequest] {
    def receive(src: NetworkDestinationHandle, msg: ClientInsertRequest): Future[Boolean] = {
      internalServer.send(partitioner.getMasterPartition(msg.k), RoutedInsertRequest(msg.k, msg.v))
    }
  }

  class FrontendGetRequestHandler extends MessageHandler[Value, ClientGetRequest] {
    def receive(src: NetworkDestinationHandle, msg: ClientGetRequest): Future[Value] = {
      internalServer.send(partitioner.getMasterPartition(msg.k), RoutedGetRequest(msg.k))
    }
  }

  /*
   * Handlers for internal routed requests
   */

  // define handlers
  class InternalPutHandler extends MessageHandler[Value, RoutedPutRequest] {
    def receive(src: NetworkDestinationHandle, msg: RoutedPutRequest): Future[Value] = {
      // returns the old value or null
      future { datastore.put(msg.k, msg.v) }
    }
  }

  class InternalGetHandler extends MessageHandler[Value, RoutedGetRequest] {
    def receive(src: NetworkDestinationHandle, msg: RoutedGetRequest): Future[Value] = {
      // returns the value or null
      future { datastore.get(msg.k) }
    }
  }

  class InternalInsertHandler extends MessageHandler[Boolean, RoutedInsertRequest] {
    def receive(src: NetworkDestinationHandle, msg: RoutedInsertRequest): Future[Boolean] = {
      // returns true if there was an old value
      future { datastore.put(msg.k, msg.v) != null }
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

case class ClientPutRequest (k: Key, v: Value) extends Request[Value]
case class ClientInsertRequest (k: Key, v: Value) extends Request[Boolean]
case class ClientGetRequest (k: Key) extends Request[Value]

case class RoutedPutRequest(k: Key, v: Value) extends Request[Value]
case class RoutedInsertRequest(k: Key, v: Value) extends Request[Boolean]
case class RoutedGetRequest(k: Key) extends Request[Value]
