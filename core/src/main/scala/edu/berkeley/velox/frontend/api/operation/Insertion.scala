package edu.berkeley.velox.datamodel.api.operation

import edu.berkeley.velox.frontend.api.Table
import edu.berkeley.velox.datamodel.{ResultSet, Row, Value, Column}
import scala.concurrent.Future

class Insertion(var table: Table, val row: Row) extends Operation {
  def into(table: Table) : Insertion = {
    this.table = table
    this
  }

  override def execute() : Future[ResultSet] = {
    table.execute(this)
  }
}
