package edu.berkeley.velox.operations.database.request

import edu.berkeley.velox.datamodel.{Predicate, Column, TableName, DatabaseName}
import edu.berkeley.velox.rpc.Request
import edu.berkeley.velox.operations.database.response.SelectionResponse

case class SelectionRequest(val database: DatabaseName,
                            val table: TableName,
                            val columns: Seq[Column],
                            val predicate: Option[Predicate] = None) extends Request[SelectionResponse]