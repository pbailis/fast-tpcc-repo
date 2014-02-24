package edu.berkeley.velox.frontend

import edu.berkeley.velox.rpc.ClientRPCService
import scala.concurrent._
import java.net.InetSocketAddress
import collection.JavaConversions._
import edu.berkeley.velox.frontend.api._
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.datamodel.api.operation._
import edu.berkeley.velox.operations.database.request._
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel.DataModelConverters._
import edu.berkeley.velox.util.NonThreadedExecutionContext.context
import scala.util.Success
import scala.util.Failure

object VeloxConnection {
  def makeConnection(addresses: java.lang.Iterable[InetSocketAddress]): VeloxConnection = {
    return new VeloxConnection(addresses)
  }
}

class VeloxConnection(serverAddresses: Iterable[InetSocketAddress]) extends Logging {
  val ms = new ClientRPCService(serverAddresses)
  ms.initialize()

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

  def select(names: ColumnLabel*) : QueryOperation = {
    new QueryOperation(null, names)
  }

  def insert(values: (ColumnLabel, Value)*) : InsertionOperation = {
    new InsertionOperation(null, values)
  }

  def execute(database: Database, table: Table, operation: Operation) : Future[ResultSet] = {
    val resultSetPromise = Promise[ResultSet]

    operation match {
      case s: QueryOperation => {
        ms.sendAny(new QueryRequest(database.name, table.name, new Query(s.columns, s.predicate))) onComplete {
          // TODO: FIX
          case Success(value) => resultSetPromise success null
          case Failure(t) => {
            logger.error("Error executing selection", t)
            resultSetPromise failure t
          }
        }
      }

      case i: InsertionOperation => {
        ms.sendAny(new InsertionRequest(database.name, table.name, i.insertSet)) onComplete {
          case Success(value) => resultSetPromise success new ResultSet
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
