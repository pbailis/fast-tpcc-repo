package edu.berkeley.velox.server

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox._
import edu.berkeley.velox.cluster.RandomPartitioner
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.datamodel.{Key, Value}
import edu.berkeley.velox.rpc.{FrontendRPCService, InternalRPCService, MessageHandler, Request}
import scala.concurrent.{Future, future}
import scala.concurrent.ExecutionContext.Implicits.global
import edu.berkeley.velox.storage.StorageManager
import edu.berkeley.velox.datamodel.Catalog


// this causes our futures to not thread
import edu.berkeley.velox.util.NonThreadedExecutionContext.context

// Every server has a single instance of this class. It handles data storage
// and serves client requests. Data is stored in a concurrent hash map.
// As requests come in and are served, the hashmap is accessed concurrently
// by the thread executing the message handlers (and the handlers have a
// reference to the map. For now, the server is oblivious to the keyrange
// it owns. It depends on the routing service to route only the correct
// keys to it.

class VeloxServer(zkConfig: ZKClient,
                  storage: StorageManager,
                  catalog: Catalog,
                  id: NetworkDestinationHandle) extends Logging {


  val servers = zkConfig.getServersInGroup()

  // Register partitioner after initializing zkConfig
  val partitioner = new RandomPartitioner(zkConfig)


  val internalServer = new InternalRPCService(id, servers)
  internalServer.initialize()

  internalServer.registerHandler(new InternalPutHandler)
  internalServer.registerHandler(new InternalGetHandler)
  internalServer.registerHandler(new InternalInsertHandler)

  // create the message service first, register handlers, then start the network
  val frontendServer = new FrontendRPCService(id)

  frontendServer.registerHandler(new FrontendPutRequestHandler)
  frontendServer.registerHandler(new FrontendPutRequestHandler)
  frontendServer.registerHandler(new FrontendGetRequestHandler)
  frontendServer.registerHandler(new FrontendAddDBRequestHandler)
  frontendServer.registerHandler(new FrontendAddTableRequestHandler)


  frontendServer.initialize()


  /*
   * Handlers for front-end requests.
   */

  class FrontendPutRequestHandler extends MessageHandler[Value, ClientPutRequest] {
    def receive(src: NetworkDestinationHandle, msg: ClientPutRequest): Future[Value] = {
      internalServer.send(partitioner.getMasterPartition(msg.k), RoutedPutRequest(msg.tname, msg.dbname, msg.k, msg.v))
    }
  }

  class FrontendInsertRequestHandler extends MessageHandler[Boolean, ClientInsertRequest] {
    def receive(src: NetworkDestinationHandle, msg: ClientInsertRequest): Future[Boolean] = {
      internalServer.send(partitioner.getMasterPartition(msg.k), RoutedInsertRequest(msg.tname, msg.dbname, msg.k, msg.v))
    }
  }

  class FrontendGetRequestHandler extends MessageHandler[Value, ClientGetRequest] {
    def receive(src: NetworkDestinationHandle, msg: ClientGetRequest): Future[Value] = {
      internalServer.send(partitioner.getMasterPartition(msg.k), RoutedGetRequest(msg.tname, msg.dbname, msg.k))
    }
  }

  class FrontendAddTableRequestHandler extends MessageHandler[Boolean, ClientAddTableRequest] {
    def receive(src: NetworkDestinationHandle, msg: ClientAddTableRequest): Future[Boolean] = {
      future {zkConfig.addToSchema(msg.dbname, Some(msg.tname))}
    }
  }

  class FrontendAddDBRequestHandler extends MessageHandler[Boolean, ClientAddDBRequest] {
    def receive(src: NetworkDestinationHandle, msg: ClientAddDBRequest): Future[Boolean] = {
      future {zkConfig.addToSchema(msg.dbname, None)}
    }
  }

  /*
   * Handlers for internal routed requests
   */
  class InternalPutHandler extends MessageHandler[Value, RoutedPutRequest] {
    def receive(src: NetworkDestinationHandle, msg: RoutedPutRequest): Future[Value] = {
      // returns the old value or null
      future {
        if (catalog.checkTableExists(msg.dbname, msg.tname)) {
          storage.put(msg.tname, msg.dbname, msg.k, msg.v)
        } else {
          logger.warn(s"Tried to PUT into ${msg.tname} in ${msg.dbname} that doesn't exist")
          null
        }
      }
    }
  }

  class InternalGetHandler extends MessageHandler[Value, RoutedGetRequest] {
    def receive(src: NetworkDestinationHandle, msg: RoutedGetRequest): Future[Value] = {
      // returns the value or null
      if (catalog.checkTableExists(msg.dbname, msg.tname)) {
        future { storage.get(msg.tname, msg.dbname, msg.k) }
      } else {
        logger.warn(s"Tried to GET from ${msg.tname} in ${msg.dbname} that doesn't exist")
        null
      }
    }
  }

  class InternalInsertHandler extends MessageHandler[Boolean, RoutedInsertRequest] {
    def receive(src: NetworkDestinationHandle, msg: RoutedInsertRequest): Future[Boolean] = {
      // returns true if there was an old value
      if (catalog.checkTableExists(msg.dbname, msg.tname)) {
        future { storage.insert(msg.tname, msg.dbname, msg.k, msg.v) }
      } else {
        logger.warn(s"Tried to INSERT into ${msg.tname} in ${msg.dbname} that doesn't exist")
        null
      }
    }
  }
}

object VeloxServer extends Logging {
  def main(args: Array[String]) {
    logger.info("Initializing Server")
    VeloxConfig.initialize(args)

    val storage = new StorageManager
    val catalog = new Catalog(storage)
    val zkClient = new ZKClient(catalog)
    val myAddr = s"${VeloxConfig.serverIpAddress}:${VeloxConfig.internalServerPort}"
    val id = zkClient.registerWithZooKeeper(myAddr)

    catalog.setZKClient(zkClient)
    // Register with ZK. initialize network service and message service
    val kvserver = new VeloxServer(zkClient, storage, catalog, id)
  }
}

case class ClientPutRequest (tname: String, dbname: String, k: Key, v: Value) extends Request[Value]
case class ClientInsertRequest (tname: String, dbname: String, k: Key, v: Value) extends Request[Boolean]
case class ClientGetRequest (tname: String, dbname: String, k: Key) extends Request[Value]
// These requests don't get routed to backend server
case class ClientAddTableRequest (tname: String, dbname: String) extends Request[Boolean]
case class ClientAddDBRequest (dbname: String) extends Request[Boolean]

case class RoutedPutRequest(tname: String, dbname: String, k: Key, v: Value) extends Request[Value]
case class RoutedInsertRequest(tname: String, dbname: String, k: Key, v: Value) extends Request[Boolean]
case class RoutedGetRequest(tname: String, dbname: String, k: Key) extends Request[Value]



