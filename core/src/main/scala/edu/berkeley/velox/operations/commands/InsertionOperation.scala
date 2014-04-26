package edu.berkeley.velox.operations.commands

import edu.berkeley.velox.datamodel._
import scala.concurrent.Future
import edu.berkeley.velox.datamodel.ColumnLabel
import edu.berkeley.velox.datamodel.DataModelConverters
import edu.berkeley.velox.operations.CommandExecutor

class InsertionOperation(val values: Seq[(ColumnLabel, Value)]) extends Operation {

  var table: QueryTable = null
  var insertSet: InsertSet = null

  def into(table: QueryTable) : InsertionOperation = {
    this.table = table
    if(values != null)
      insertSet = DataModelConverters.toInsertSetSeq(values)(table.schema)
    this
  }

  def insert(newValues: (ColumnLabel, Value)*): InsertionOperation = {
    val is = DataModelConverters.toInsertSetSeq(newValues)(table.schema)
    if(insertSet != null)
      insertSet.merge(is)
    else
      insertSet = is
    this
  }

  override def execute() : Future[ResultSet] = {
    if (table == null)
      throw new Exception("Insert with no table specified")
    table.execute(this)
  }
}
