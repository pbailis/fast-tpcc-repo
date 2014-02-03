package edu.berkeley.velox.datamodel

import edu.berkeley.velox.frontend.api.Table
import edu.berkeley.velox.datamodel.api.operation.Operation
import scala.concurrent.Future

case class Query(val columns: Seq[ColumnLabel], val predicate: Option[Predicate])
