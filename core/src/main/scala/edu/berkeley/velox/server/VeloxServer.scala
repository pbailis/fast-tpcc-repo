package edu.berkeley.velox.server

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox._
import edu.berkeley.velox.cluster.RandomPartitioner
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.rpc.{FrontendRPCService, InternalRPCService, MessageHandler, Request}
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{Promise, Future, future}
import scala.concurrent.ExecutionContext.Implicits.global
import edu.berkeley.velox.operations.database.request._
import edu.berkeley.velox.operations.database.response.{QueryResponse, InsertionResponse, CreateTableResponse, CreateDatabaseResponse}
import edu.berkeley.velox.storage.StorageManager
import edu.berkeley.velox.catalog.SystemCatalog
import edu.berkeley.velox.datamodel.{InsertSet, ResultSet, Row}
import scala.util.{Failure, Success}
import scala.util.Failure
import edu.berkeley.velox.operations.database.request.CreateDatabaseRequest
import edu.berkeley.velox.operations.database.request.CreateTableRequest
import edu.berkeley.velox.operations.database.response.QueryResponse
import scala.util.Success
import edu.berkeley.velox.operations.database.request.InsertionRequest
import scala.collection.mutable
import scala.collection.JavaConversions


// Every server has a single instance of this class. It handles data storage
// and serves client requests. Data is stored in a concurrent hash map.
// As requests come in and are served, the hashmap is accessed concurrently
// by the thread executing the message handlers (and the handlers have a
// reference to the map. For now, the server is oblivious to the keyrange
// it owns. It depends on the routing service to route only the correct
// keys to it.

class VeloxServer extends Logging {
  val catalog = new SystemCatalog
  val storage = new StorageManager(catalog)

  val partitioner = new RandomPartitioner

  val internalServer = new InternalRPCService
  internalServer.initialize()

  internalServer.registerHandler(new InternalCreateDatabaseRequestHandler)
  internalServer.registerHandler(new InternalCreateTableRequestHandler)
  internalServer.registerHandler(new InternalQueryRequestHandler)
  internalServer.registerHandler(new InternalInsertionRequestHandler)

  // create the message service first, register handlers, then start the network
  val frontendServer = new FrontendRPCService

  frontendServer.registerHandler(new FrontendCreateDatabaseRequestHandler)
  frontendServer.registerHandler(new FrontendCreateTableRequestHandler)
  frontendServer.registerHandler(new FrontendQueryRequestHandler)
  frontendServer.registerHandler(new FrontendInsertionRequestHandler)

  frontendServer.initialize()

  /*
   * Handlers for front-end requests.
   */

  class FrontendCreateDatabaseRequestHandler extends MessageHandler[CreateDatabaseResponse, CreateDatabaseRequest] {
    def receive(src: NetworkDestinationHandle, msg: CreateDatabaseRequest): Future[CreateDatabaseResponse] = {
      val p = Promise[CreateDatabaseResponse]
      val f = Future.sequence(internalServer.sendAll(msg))
      f onComplete {
        case Success(responses) =>
          p success new CreateDatabaseResponse
        case Failure(t) => {
          logger.error(s"Error creating database ${msg.name}", t)
          p failure t
        }
      }
      p.future
    }
  }

  class FrontendCreateTableRequestHandler extends MessageHandler[CreateTableResponse, CreateTableRequest] {
    def receive(src: NetworkDestinationHandle, msg: CreateTableRequest): Future[CreateTableResponse] = {
      val p = Promise[CreateTableResponse]
      val f = Future.sequence(internalServer.sendAll(msg))
      f onComplete {
        case Success(responses) =>
          p success new CreateTableResponse
        case Failure(t) => {
          logger.error(s"Error creating table ${msg.database}, database ${msg.table}", t)
          p failure t
        }
      }
      p.future
    }
  }

  class FrontendInsertionRequestHandler extends MessageHandler[InsertionResponse, InsertionRequest] {
    def receive(src: NetworkDestinationHandle, msg: InsertionRequest): Future[InsertionResponse] = {
      val p = Promise[InsertionResponse]

      val requestsByPartition: Map[NetworkDestinationHandle, InsertSet] = Map().withDefaultValue(new InsertSet)

      msg.insertSet.getRows.foreach(
        r => {
          val pkey = catalog.extractPrimaryKey(msg.database, msg.table, r)
          val partition = partitioner.getMasterPartition(pkey)
          requestsByPartition(partition).appendRow(r)
        }
      )

      val f = Future.sequence(requestsByPartition.map {
        case (destination, insertSet) => internalServer.send(destination, new InsertionRequest(msg.database, msg.table, insertSet))
      })

      f onComplete {
        case Success(responses) =>
          p success new InsertionResponse
        case Failure(t) =>
          logger.error("Error processing insertion", t)
          p failure t
      }

      p.future
    }
  }

  class FrontendQueryRequestHandler extends MessageHandler[QueryResponse, QueryRequest] {
    def receive(src: NetworkDestinationHandle, msg: QueryRequest): Future[QueryResponse] = {
      val p = Promise[QueryResponse]
      val f = Future.sequence(internalServer.sendAll(msg))
      f onComplete {
        case Success(responses) =>
          var ret = new ResultSet
          responses foreach { r => ret.merge(r.results) }
          p success new QueryResponse(ret)
        case Failure(t) => {
          logger.error(s"Error processing query", t)
          p failure t
        }
      }
      p.future
    }
  }

  /*
   * Handlers for internal routed requests
   */

  class InternalInsertionRequestHandler extends MessageHandler[InsertionResponse, InsertionRequest] {
    def receive(src: NetworkDestinationHandle, msg: InsertionRequest): Future[InsertionResponse] = {
      future {
        storage.insert(msg.database, msg.table, msg.insertSet)
        new InsertionResponse
      }
    }
  }

  class InternalQueryRequestHandler extends MessageHandler[QueryResponse, QueryRequest] {
    def receive(src: NetworkDestinationHandle, msg: QueryRequest): Future[QueryResponse] = {
      future {
        new QueryResponse(storage.query(msg.database, msg.table, msg.query))
      }
    }
  }

  class InternalCreateDatabaseRequestHandler extends MessageHandler[CreateDatabaseResponse, CreateDatabaseRequest] {
    def receive(src: NetworkDestinationHandle, msg: CreateDatabaseRequest): Future[CreateDatabaseResponse] = {
      future {
        catalog.createDatabase(msg.name)
        storage.createDatabase(msg.name)
        new CreateDatabaseResponse
      }
    }
  }

  class InternalCreateTableRequestHandler extends MessageHandler[CreateTableResponse, CreateTableRequest] {
    def receive(src: NetworkDestinationHandle, msg: CreateTableRequest): Future[CreateTableResponse] = {
      future {
        catalog.createTable(msg.database, msg.table, msg.schema)
        storage.createTable(msg.database, msg.table)
        new CreateTableResponse
      }
    }
  }
}

object VeloxServer extends Logging {
  def main(args: Array[String]) {
    logger.info("Initializing Server")
    VeloxConfig.initialize(args)
    new VeloxServer
  }
}
