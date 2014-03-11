package edu.berkeley.velox.server

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox._
import edu.berkeley.velox.cluster.ServerRandomPartitioner
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.rpc.{FrontendRPCService, InternalRPCService, MessageHandler}
import java.util.{HashMap => JHashMap}
import scala.concurrent.{Promise, Future, future}
import edu.berkeley.velox.operations.database.request._
import edu.berkeley.velox.operations.database.response.{InsertionResponse, CreateTableResponse, CreateDatabaseResponse}
import edu.berkeley.velox.storage.StorageManager
import edu.berkeley.velox.util.NonThreadedExecutionContext.context
import edu.berkeley.velox.datamodel.{InsertSet, ResultSet}
import scala.util.Failure
import edu.berkeley.velox.operations.database.request.CreateDatabaseRequest
import edu.berkeley.velox.operations.database.request.CreateTableRequest
import edu.berkeley.velox.operations.database.response.QueryResponse
import scala.util.Success
import edu.berkeley.velox.operations.database.request.InsertionRequest
import scala.collection.JavaConverters._
import edu.berkeley.velox.catalog.{ClientCatalog, ServerCatalog}
import java.util

class VeloxServer(storage: StorageManager,
                  id: NetworkDestinationHandle) extends Logging {

  val servers = ServerZookeeperConnection.getServersInGroup()
  val partitioner = new ServerRandomPartitioner
  //ServerCatalog.initializeSchemaFromZK()

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
