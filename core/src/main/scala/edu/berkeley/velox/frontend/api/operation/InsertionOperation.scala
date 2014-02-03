package edu.berkeley.velox.datamodel.api.operation

import edu.berkeley.velox.frontend.api.Table
import edu.berkeley.velox.datamodel._
import scala.concurrent.Future
import edu.berkeley.velox.datamodel.ColumnLabel
import edu.berkeley.velox.datamodel.DataModelConverters._

class InsertionOperation(var table: Table, val insertSet: InsertSet) extends Operation {
  def into(table: Table) : InsertionOperation = {
    this.table = table
    this
  }

  def insert(values: (ColumnLabel, Value)*): InsertionOperation = {
    insertSet.merge(values)
    this
  }

  override def execute() : Future[ResultSet] = {
    table.execute(this)
  }
}
