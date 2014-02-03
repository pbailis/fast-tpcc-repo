package edu.berkeley.velox.operations.database.request

import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.rpc.Request
import edu.berkeley.velox.operations.database.response.{InsertionResponse, SelectionResponse}
import edu.berkeley.velox.datamodel.Column

case class InsertionRequest(val database: DatabaseName,
                            val table: TableName,
                            val row: Row) extends Request[InsertionResponse]
