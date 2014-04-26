package edu.berkeley.velox.frontend

import edu.berkeley.velox.rpc.ClientRPCService
import scala.concurrent._
import edu.berkeley.velox.datamodel._
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.util.NonThreadedExecutionContext.context
import edu.berkeley.velox._
import edu.berkeley.velox.util.JVMClassClosureUtil
import edu.berkeley.velox.catalog.Catalog
import java.util.{HashMap => JHashMap}
import scala.collection.JavaConverters._


import java.util
import edu.berkeley.velox.operations.CommandExecutor
import edu.berkeley.velox.udf.PerPartitionUDF
import edu.berkeley.velox.operations.commands._
import scala.util.Failure
import edu.berkeley.velox.datamodel.ColumnLabel
import edu.berkeley.velox.operations.commands.Operation
import scala.util.Success
import edu.berkeley.velox.datamodel.Query
import edu.berkeley.velox.operations.internal.{InsertionResponse, InsertionRequest, QueryRequest, PerPartitionUDFRequest}
import scala.concurrent.duration.Duration


object VeloxConnection {
  def makeConnection: VeloxConnection = {
    new VeloxConnection
  }
}

class VeloxConnection extends Logging with CommandExecutor {

  val ms = new ClientRPCService
  ms.initialize()
  Catalog.initCatalog(isClient=true)
  Catalog.initializeSchemaFromZK()

  val partitioner = new ClientRandomPartitioner

  def perPartitionUDF(udf: PerPartitionUDF): Future[Seq[Any]] = {
    return Future.sequence(ms.sendAllRemote(new PerPartitionUDFRequest(udf)))
  }

  // Routing
  override def execute(database: QueryDatabase, table: QueryTable, operation: Operation) : Future[ResultSet] = {
    val results: Future[ResultSet] = operation match {
      case s: QueryOperation => {
        executeQuery(new QueryRequest(new Query(database.name, table.name, s.columns, s.predicates)))
      }
      case i: InsertionOperation => {
        executeInsert(new InsertionRequest(database.name, table.name, i.insertSet))
      }
    }
    results
  }

  // Routing
  override def executeBlocking(database: QueryDatabase, table: QueryTable, operation: Operation) : ResultSet = {
    val resultsFuture = execute(database, table, operation)
    Await.ready(resultsFuture, Duration.Inf)
    resultsFuture.value.get.get
  }

  private def executeQuery(query: QueryRequest) : Future[ResultSet] = {
    val resultSetPromise = Promise[ResultSet]
    val f = Future.sequence(ms.sendAllRemote(query))
    f onComplete {
      case Success(responses) => {
        val results = new ResultSet
        responses foreach { r => results.merge(r.results) }
        resultSetPromise success results
      }
      case Failure(t) => {
        logger.error(s"Error processing query", t)
        resultSetPromise failure t
      }
    }
    resultSetPromise.future
  }

  private def executeInsert(insertion: InsertionRequest) : Future[ResultSet] = {
    val resultSetPromise = Promise[ResultSet]
    val requestsByPartition = new JHashMap[NetworkDestinationHandle, InsertSet]
    insertion.insertSet.getRows.foreach(
      r => {
        val pkey = Catalog.extractPrimaryKey(insertion.database, insertion.table, r)
        val partition = partitioner.getMasterPartition(pkey)
        if(!requestsByPartition.containsKey(partition)) {
          requestsByPartition.put(partition, new InsertSet)
        }
        requestsByPartition.get(partition).appendRow(r)
      }
    )
    val insertFutures = new util.ArrayList[Future[InsertionResponse]](requestsByPartition.size)
    val rbp_it = requestsByPartition.entrySet.iterator()
    while(rbp_it.hasNext) {
      val rbp = rbp_it.next()
      insertFutures.add(ms.send(rbp.getKey, new InsertionRequest(insertion.database, insertion.table, rbp.getValue)))
    }
    val f = Future.sequence(insertFutures.asScala)
    f onComplete {
      case Success(responses) =>
        // Response is empty ResultSet
        resultSetPromise success new ResultSet
      case Failure(t) => {
        logger.error("Error processing insertion", t)
        resultSetPromise failure t
      }
    }
    resultSetPromise.future
  }
}
