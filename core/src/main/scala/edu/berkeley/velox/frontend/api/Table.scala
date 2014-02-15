package edu.berkeley.velox.frontend.api

import edu.berkeley.velox.datamodel.api.operation.{Operation, InsertionOperation, QueryOperation}
import edu.berkeley.velox.datamodel.{ResultSet, Value, Row, ColumnLabel}
import edu.berkeley.velox.datamodel.DataModelConverters._
import scala.concurrent.Future

class Table(val database: Database, val name: String) {
  def column(name: String) : ColumnLabel = {
    name
  }

  def insert(values: (ColumnLabel, Value)*) : InsertionOperation = {
    new InsertionOperation(this, values)
  }

  def select(names: ColumnLabel*) : QueryOperation = {
    new QueryOperation(this, names)
  }

  def execute(operation: Operation) : Future[ResultSet] = {
    database.execute(this, operation)
  }
}
