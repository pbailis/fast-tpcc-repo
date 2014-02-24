package edu.berkeley.velox.datamodel.api.operation

import edu.berkeley.velox.datamodel.{ResultSet, ColumnLabel, Predicate}
import edu.berkeley.velox.frontend.api.Table
import scala.concurrent.Future

class QueryOperation(var table: Table, val columns: Seq[ColumnLabel]) extends Operation {
  var predicate: Option[Predicate] = None;

  override def execute() : Future[ResultSet] = {
    table.execute(this)
  }

  def where(pred: Predicate) : QueryOperation = {
    predicate = Some(pred)
    this
  }

  def from(table: Table) : QueryOperation = {
    this.table = table
    this
  }
}
