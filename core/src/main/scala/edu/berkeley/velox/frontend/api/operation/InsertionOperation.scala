package edu.berkeley.velox.datamodel.api.operation

import edu.berkeley.velox.frontend.api.Table
import edu.berkeley.velox.datamodel._
import scala.concurrent.Future
import edu.berkeley.velox.datamodel.ColumnLabel
import edu.berkeley.velox.datamodel.DataModelConverters

class InsertionOperation(val values: Seq[(ColumnLabel, Value)]) extends Operation {

  var table: Table = null
  var insertSet: InsertSet = null

  def into(table: Table) : InsertionOperation = {
    this.table = table
    insertSet = DataModelConverters.toInsertSetSeq(values)(table.schema)
    this
  }

  def insert(values: (ColumnLabel, Value)*): InsertionOperation = {
    val is = DataModelConverters.toInsertSetSeq(values)(table.schema)
    insertSet.merge(is)
    this
  }

  override def execute() : Future[ResultSet] = {
    if (table == null)
      throw new Exception("Insert with no table specified")
    table.execute(this)
  }
}
