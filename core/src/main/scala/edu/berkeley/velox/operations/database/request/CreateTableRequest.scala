package edu.berkeley.velox.operations.database.request

import edu.berkeley.velox.datamodel.{Schema, TableName, DatabaseName}
import edu.berkeley.velox.operations.database.response.CreateTableResponse
import edu.berkeley.velox.rpc.Request

case class CreateTableRequest(val database: DatabaseName,
                              val table: TableName,
                              val schema: Schema) extends Request[CreateTableResponse]