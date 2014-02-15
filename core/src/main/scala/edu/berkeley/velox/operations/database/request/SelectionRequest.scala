package edu.berkeley.velox.operations.database.request

import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.rpc.Request
import edu.berkeley.velox.operations.database.response.QueryResponse
import edu.berkeley.velox.operations.database.response.QueryResponse

case class QueryRequest(val database: DatabaseName,
                        val table: TableName,
                        val query: Query) extends Request[QueryResponse]
