package edu.berkeley.velox.frontend.api

import edu.berkeley.velox.catalog.Catalog
import edu.berkeley.velox.datamodel.{ColumnLabel, ResultSet, Row, Value}
import edu.berkeley.velox.datamodel.DataModelConverters.stringToColumnID
import edu.berkeley.velox.datamodel.InsertSet
import edu.berkeley.velox.datamodel.api.operation.{InsertionOperation, Operation, QueryOperation}
import edu.berkeley.velox.exceptions.QueryException
import scala.concurrent.Future

class Table(val database: Database, val name: String) {

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

  def select(names: ColumnLabel*) : QueryOperation = {
    new QueryOperation(this, names)
  }

  def execute(operation: Operation) : Future[ResultSet] = {
    database.execute(this, operation)
  }
}
