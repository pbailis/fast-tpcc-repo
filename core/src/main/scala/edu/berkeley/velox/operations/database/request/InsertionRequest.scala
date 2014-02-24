package edu.berkeley.velox.operations.database.request

import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.rpc.Request
import edu.berkeley.velox.operations.database.response.{InsertionResponse, QueryResponse}
import edu.berkeley.velox.datamodel.ColumnLabel

case class InsertionRequest(val database: DatabaseName,
                            val table: TableName,
                            val insertSet: InsertSet) extends Request[InsertionResponse]
