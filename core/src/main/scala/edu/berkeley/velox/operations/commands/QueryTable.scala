package edu.berkeley.velox.operations.commands

import edu.berkeley.velox.catalog.Catalog
import edu.berkeley.velox.datamodel.{Query, ColumnLabel, ResultSet, Value}
import edu.berkeley.velox.datamodel.DataModelConverters.stringToColumnID
import edu.berkeley.velox.exceptions.QueryException
import scala.concurrent.Future
import edu.berkeley.velox.operations.CommandExecutor

class QueryTable(val database: QueryDatabase, val name: String) {
  val schema = Catalog.getSchema(database.name,name)

  if (schema == null)
    throw new QueryException(s"Table $name does not exist in database $database")

  // uses stringToColumnID
  def column(name: String) : ColumnLabel = name

  def insert(values: (ColumnLabel, Value)*) : InsertionOperation = {
    val ret = new InsertionOperation(values)
    ret.into(this)
    ret
  }

  def insertBatch(): InsertionOperation = {
    new InsertionOperation(null).into(this)
  }

  def select(names: ColumnLabel*) : QueryOperation = {
    new QueryOperation(this, names)
  }

  def execute(operation: Operation) : Future[ResultSet] = {
    database.execute(this, operation)
  }

  def executeBlocking(operation: Operation) : ResultSet = {
    database.executeBlocking(this, operation)
  }

  def prepareQuery(queryOperation: QueryOperation): Query = {
    database.prepareQuery(this, queryOperation)
  }
}
