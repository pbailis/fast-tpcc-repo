package edu.berkeley.velox.datamodel.api.operation

import edu.berkeley.velox.datamodel.{ResultSet, Column, Predicate}
import edu.berkeley.velox.frontend.api.Table
import scala.concurrent.Future

class Selection(var table: Table, val columns: Seq[Column]) extends Operation {
  var predicate: Option[Predicate] = None;

  override def execute() : Future[ResultSet] = {
    table.execute(this)
  }

  def where(pred: Predicate) : Selection = {
    predicate = Some(pred)
    this
  }

  def from(table: Table) : Selection = {
    this.table = table
    this
  }
}
