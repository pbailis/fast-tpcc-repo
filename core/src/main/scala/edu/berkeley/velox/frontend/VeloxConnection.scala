package edu.berkeley.velox.frontend

import edu.berkeley.velox.rpc.{ClientRPCService, Request}
import scala.concurrent._
import scala.concurrent.duration._
import edu.berkeley.velox.server._
import java.net.InetSocketAddress
import collection.JavaConversions._
import edu.berkeley.velox.frontend.api.{Table, Database}
import edu.berkeley.velox.datamodel.api.operation.{Insertion, Selection, Operation}
import edu.berkeley.velox.operations.database.request.{InsertionRequest, SelectionRequest, CreateTableRequest, CreateDatabaseRequest}
import edu.berkeley.velox.datamodel._
import scala.util.{Success, Failure}
import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Success
import edu.berkeley.velox.operations.database.request.SelectionRequest
import edu.berkeley.velox.operations.database.request.InsertionRequest
import scala.util.Failure
import edu.berkeley.velox.datamodel.Schema
import edu.berkeley.velox.operations.database.request.CreateDatabaseRequest
import edu.berkeley.velox.operations.database.request.CreateTableRequest
import edu.berkeley.velox.datamodel.Column
import edu.berkeley.velox.datamodel.RowConversion._

object VeloxConnection {
  def makeConnection(addresses: java.lang.Iterable[InetSocketAddress]): VeloxConnection = {
    return new VeloxConnection(addresses)
  }
}

class VeloxConnection(serverAddresses: Iterable[InetSocketAddress]) extends Logging {
  val ms = new ClientRPCService(serverAddresses)
  ms.initialize()
  ms.connect(serverAddresses)

  def database(name: DatabaseName) : Database = {
    // TODO: check if exists?
    new Database(this, name)
  }

  def createDatabase(name: DatabaseName) : Future[Database] = {
    val df = Promise[Database]

    ms.sendAny(new CreateDatabaseRequest(name)) onComplete {
      case Success(value) => df success new Database(this, name)
      case Failure(t) => {
        logger.error("Error creating database", t)
        df failure t
      }
    }

    df.future
  }

  def createTable(database: Database, tableName: TableName, schema: Schema) : Future[Table] = {
    val df = Promise[Table]

    ms.sendAny(new CreateTableRequest(database.name, tableName, schema)) onComplete {
      case Success(value) => df success new Table(database, tableName)
      case Failure(t) => {
        logger.error("Error creating table", t)
        df failure t
      }
    }

    df.future
  }

  def select(names: Column*) : Selection = {
    new Selection(null, names)
  }

  def insert(values: (Column, Value)*) : Insertion = {
    new Insertion(null, values)
  }

  def execute(database: Database, table: Table, operation: Operation) : Future[ResultSet] = {
    val resultSetPromise = Promise[ResultSet]

    operation match {
      case s: Selection => {
        ms.sendAny(new SelectionRequest(database.name, table.name, s.columns, s.predicate)) onComplete {
          // TODO: FIX
          case Success(value) => resultSetPromise success null
          case Failure(t) => {
            logger.error("Error executing selection", t)
            resultSetPromise failure t
          }
        }
      }

      case i: Insertion => {
        ms.sendAny(new InsertionRequest(database.name, table.name, i.row)) onComplete {
          case Success(value) => resultSetPromise success ResultSet.EMPTY
          case Failure(t) => {
            logger.error("Error executing insertion", t)
            resultSetPromise failure t
          }
        }
      }
    }

    resultSetPromise.future
  }
}
