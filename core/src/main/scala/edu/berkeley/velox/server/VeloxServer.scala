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

//  internalServer.registerHandler(new InternalQueryRequestHandler)
//  internalServer.registerHandler(new InternalInsertionRequestHandler)

  // create the message service first, register handlers, then start the network
  val frontendServer = new FrontendRPCService(id)

//  frontendServer.registerHandler(new FrontendCreateDatabaseRequestHandler)
//  frontendServer.registerHandler(new FrontendCreateTableRequestHandler)
  frontendServer.registerHandler(new QueryRequestHandler)
  frontendServer.registerHandler(new InsertionRequestHandler)

  frontendServer.initialize()
  logger.warn("Frontend server initialized.")


  /*
   * Handlers for front-end requests.
   */

//  class FrontendCreateDatabaseRequestHandler extends MessageHandler[CreateDatabaseResponse, CreateDatabaseRequest] {
//    def receive(src: NetworkDestinationHandle, msg: CreateDatabaseRequest): Future[CreateDatabaseResponse] = {
//      future {
//        ServerCatalog.createDatabase(msg.name)
//        new CreateDatabaseResponse
//      }
//    }
//  }
//
//  class FrontendCreateTableRequestHandler extends MessageHandler[CreateTableResponse, CreateTableRequest] {
//    def receive(src: NetworkDestinationHandle, msg: CreateTableRequest): Future[CreateTableResponse] = {
//      future {
//        ServerCatalog.createTable(msg.database, msg.table, msg.schema)
//        new CreateTableResponse
//      }
//    }
//  }

//  class FrontendInsertionRequestHandler extends MessageHandler[InsertionResponse, InsertionRequest] {
//    def receive(src: NetworkDestinationHandle, msg: InsertionRequest): Future[InsertionResponse] = {
//      val p = Promise[InsertionResponse]
//
//      val requestsByPartition = new JHashMap[NetworkDestinationHandle, InsertSet]
//
//      msg.insertSet.getRows.foreach(
//        r => {
//          val pkey = ServerCatalog.extractPrimaryKey(msg.database, msg.table, r)
//          val partition = partitioner.getMasterPartition(pkey)
//          if(!requestsByPartition.containsKey(partition)) {
//            requestsByPartition.put(partition, new InsertSet)
//          }
//
//          requestsByPartition.get(partition).appendRow(r)
//        }
//      )
//
//      val insertFutures = new util.ArrayList[Future[InsertionResponse]](requestsByPartition.size)
//
//      val rbp_it = requestsByPartition.entrySet.iterator()
//      while(rbp_it.hasNext) {
//        val rbp = rbp_it.next()
//        insertFutures.add(internalServer.send(rbp.getKey, new InsertionRequest(msg.database, msg.table, rbp.getValue)))
//      }
//
//      val f = Future.sequence(insertFutures.asScala)
//
//      f onComplete {
//        case Success(responses) =>
//          p success new InsertionResponse
//        case Failure(t) => {
//          logger.error("Error processing insertion", t)
//          p failure t
//        }
//      }
//
//      p.future
//    }
//  }
//
//  class FrontendQueryRequestHandler extends MessageHandler[QueryResponse, QueryRequest] {
//    def receive(src: NetworkDestinationHandle, msg: QueryRequest): Future[QueryResponse] = {
//      val p = Promise[QueryResponse]
//      val f = Future.sequence(internalServer.sendAll(msg))
//      f onComplete {
//        case Success(responses) => {
//          var ret = new ResultSet
//          responses foreach { r => ret.merge(r.results) }
//          p success new QueryResponse(ret)
//        }
//        case Failure(t) => {
//          logger.error(s"Error processing query", t)
//          p failure t
//        }
//      }
//      p.future
//    }
//  }

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
