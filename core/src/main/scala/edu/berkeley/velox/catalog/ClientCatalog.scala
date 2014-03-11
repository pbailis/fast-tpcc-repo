package edu.berkeley.velox.catalog

import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.server.ServerZookeeperConnection
import edu.berkeley.velox.frontend.ClientZookeeperConnection
import scala.concurrent.{Promise, Future, future}

import edu.berkeley.velox.frontend.api.{Table, Database}

/**
 * Created on 3/4/14.
 */
object ClientCatalog extends Catalog {
  def createDatabase(dbName: DatabaseName): Boolean = {
    logger.info(s"Creating database $dbName")
    // This call ensures sure the DB has been added to our local schema
    _createDatabaseTrigger(dbName)
    ClientZookeeperConnection.createDatabase(dbName)
  }

  def createTable(dbName: DatabaseName, tableName: TableName, schema: Schema): Boolean = {
    logger.info(s"Creating table $dbName:$tableName")
    _createTableTrigger(dbName, tableName, schema)
    ClientZookeeperConnection.createTable(dbName, tableName, schema)
  }
}
