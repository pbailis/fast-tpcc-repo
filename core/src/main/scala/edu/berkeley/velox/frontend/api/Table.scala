package edu.berkeley.velox.frontend.api

import edu.berkeley.velox.datamodel.api.operation.{Operation, Insertion, Selection}
import edu.berkeley.velox.datamodel.{ResultSet, Value, Row, Column}
import edu.berkeley.velox.datamodel.RowConversion._
import scala.concurrent.Future

class Table(val database: Database, val name: String) {
  def column(name: String) : Column = {
    name
  }

  def insert(values: (Column, Value)*) : Insertion = {
    new Insertion(this, values)
  }

  def select(names: Column*) : Selection = {
    new Selection(this, names)
  }

  def execute(operation: Operation) : Future[ResultSet] = {
    database.execute(this, operation)
  }
}
