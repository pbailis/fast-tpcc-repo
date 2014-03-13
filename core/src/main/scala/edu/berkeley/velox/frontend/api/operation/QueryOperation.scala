package edu.berkeley.velox.datamodel.api.operation

import edu.berkeley.velox.datamodel.{ResultSet, ColumnLabel, Predicate}
import edu.berkeley.velox.exceptions.QueryException
import edu.berkeley.velox.frontend.api.Table
import scala.concurrent.Future

class QueryOperation(var table: Table, val columns: Seq[ColumnLabel]) extends Operation {
  var predicates: Seq[Predicate] = Nil

  override def execute(): Future[ResultSet] = {
    table.execute(this)
  }

  def where(pred: Predicate): QueryOperation = {
    predicates = Seq(pred)
    this
  }

  def from(table: Table): QueryOperation = {
    this.table = table
    this
  }

  def and(pred:Predicate): QueryOperation = {
    if (predicates == Nil)
      throw new QueryException("Syntax error, and not expected here")
    predicates :+= pred
    this
  }
}
