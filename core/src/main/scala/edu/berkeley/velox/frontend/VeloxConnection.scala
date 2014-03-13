package edu.berkeley.velox.frontend

import edu.berkeley.velox.rpc.ClientRPCService
import scala.concurrent._
import edu.berkeley.velox.frontend.api._
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.datamodel.api.operation._
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel.DataModelConverters._
import edu.berkeley.velox.util.NonThreadedExecutionContext.context
import edu.berkeley.velox.operations.database.response.InsertionResponse
import edu.berkeley.velox._
import edu.berkeley.velox.util.ClosureUtil
import scala.util.Failure
import edu.berkeley.velox.datamodel.ColumnLabel
import scala.util.Success
import edu.berkeley.velox.operations.database.request.InsertionRequest
import edu.berkeley.velox.operations.database.request.QueryRequest
import edu.berkeley.velox.datamodel.Query
import edu.berkeley.velox.catalog.Catalog
import java.util.{HashMap => JHashMap}
import scala.collection.JavaConverters._


import java.util


object VeloxConnection {
  def makeConnection: VeloxConnection = {
    new VeloxConnection
  }
}

class VeloxConnection extends Logging {

  val ms = new ClientRPCService
  ms.initialize()
  Catalog.initCatalog(isClient=true)
  Catalog.initializeSchemaFromZK()

  val partitioner = new ClientRandomPartitioner

  def database(name: DatabaseName): Database = {
    new Database(this, name)
  }

  def createDatabase(name: DatabaseName) : Future[Database] = {
    future {
      Catalog.createDatabase(name)
      new Database(this, name)
    }
  }

  // blocking command to register a new trigger.
  def registerTrigger(dbName: DatabaseName, tableName: TableName, triggerClass: Class[_]) {
    val className = triggerClass.getName
    val classBytes = ClosureUtil.classToBytes(triggerClass)
    Catalog.registerTrigger(dbName, tableName, className, classBytes)
  }

  def createTable(database: Database, tableName: TableName, schema: Schema) : Future[Table] = {
    future {
      Catalog.createTable(database.name, tableName, schema)
      new Table(database, tableName)
    }
  }


  // Query creation/parsing/validation
  def select(names: ColumnLabel*) : QueryOperation = {
    new QueryOperation(null, names)
  }

  def insert(values: (ColumnLabel, Value)*) : InsertionOperation = {
    new InsertionOperation(values)
  }

  // Routing
  def execute(database: Database, table: Table, operation: Operation) : Future[ResultSet] = {

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
