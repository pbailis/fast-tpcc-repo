package edu.berkeley.velox.server

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox._
import edu.berkeley.velox.cluster.ServerRandomPartitioner
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.rpc.{FrontendRPCService, InternalRPCService, MessageHandler}
import java.util.{HashMap => JHashMap}
import scala.concurrent.{Promise, Future, future}
import edu.berkeley.velox.operations.database.request._
import edu.berkeley.velox.operations.database.response._
import edu.berkeley.velox.storage.StorageManager
import edu.berkeley.velox.util.NonThreadedExecutionContext.context
import edu.berkeley.velox.datamodel.{InsertSet, ResultSet}
import scala.util.Failure
import scala.util.Success
import scala.collection.JavaConverters._
import edu.berkeley.velox.catalog.Catalog
import java.util
import edu.berkeley.velox.trigger.TriggerManager

// Every server has a single instance of this class. It handles data storage
// and serves client requests. Data is stored in a concurrent hash map.
// As requests come in and are served, the hashmap is accessed concurrently
// by the thread executing the message handlers (and the handlers have a
// reference to the map. For now, the server is oblivious to the keyrange
// it owns. It depends on the routing service to route only the correct
// keys to it.

class VeloxServer(storage: StorageManager,
                  id: NetworkDestinationHandle) extends Logging {

  Catalog.initCatalog(isClient=false)
  val servers = ServerZookeeperConnection.getServersInGroup()
  val partitioner = new ServerRandomPartitioner
  //Catalog.initializeSchemaFromZK()

  // internalServer is unused for now
  val internalServer = new InternalRPCService(id, servers)
  internalServer.initialize()
  logger.info("Internal server initialized")

  // create the message service first, register handlers, then start the network
  val frontendServer = new FrontendRPCService(id)

  frontendServer.registerHandler(new QueryRequestHandler)
  frontendServer.registerHandler(new InsertionRequestHandler)

  frontendServer.initialize()
  logger.warn("Frontend server initialized.")

  TriggerManager.initialize(internalServer)

  /*
   * Handlers for client requests
   */

  class InsertionRequestHandler extends MessageHandler[InsertionResponse, InsertionRequest] with Logging {
    def receive(src: NetworkDestinationHandle, msg: InsertionRequest): Future[InsertionResponse] = {
      future {
        storage.insert(msg.database, msg.table, msg.insertSet)
        new InsertionResponse
      }
    }
  }

  class QueryRequestHandler extends MessageHandler[QueryResponse, QueryRequest] with Logging {
    def receive(src: NetworkDestinationHandle, msg: QueryRequest): Future[QueryResponse] = {
      future {
        new QueryResponse(storage.query(msg.database, msg.table, msg.query))
      }
    }
  }
}

object VeloxServer extends Logging {
  def main(args: Array[String]) {
    logger.info("Initializing Server")
    VeloxConfig.initialize(args)

    val storage = VeloxConfig.getStorageManager
    val myAddr =
      new ServerAddress(VeloxConfig.serverIpAddress, VeloxConfig.internalServerPort, VeloxConfig.externalServerPort)
    val id = ServerZookeeperConnection.registerWithZooKeeper(myAddr)
    logger.info(s"Registered with Zookeeper, assigned ID: $id")
    // Register with ZK. initialize network service and message service
    val kvserver = new VeloxServer(storage, id)
  }
}
