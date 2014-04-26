package edu.berkeley.velox.operations.commands

import edu.berkeley.velox.datamodel.{Query, ResultSet, ColumnLabel, Predicate}
import edu.berkeley.velox.exceptions.QueryException
import scala.concurrent.Future
import edu.berkeley.velox.operations.CommandExecutor

class QueryOperation(var table: QueryTable, val columns: Seq[ColumnLabel]) extends Operation {
  var predicates: Seq[Predicate] = Nil

  override def execute(): Future[ResultSet] = {
    table.execute(this)
  }

  def where(pred: Predicate): QueryOperation = {
    predicates = Seq(pred)
    this
  }

  def from(table: QueryTable): QueryOperation = {
    this.table = table
    this
  }

  def and(pred:Predicate): QueryOperation = {
    if (predicates == Nil)
      throw new QueryException("Syntax error, and not expected here")
    predicates :+= pred
    this
  }

  def prepareQuery: Query = {
    table.prepareQuery(this)
  }
}
