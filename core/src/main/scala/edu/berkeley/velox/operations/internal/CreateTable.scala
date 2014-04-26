package edu.berkeley.velox.operations.internal

import edu.berkeley.velox.datamodel.{Schema, TableName, DatabaseName}
import edu.berkeley.velox.rpc.Request

private[velox] case class CreateTableRequest(val database: DatabaseName,
                                             val table: TableName,
                                             val schema: Schema) extends Request[CreateTableResponse]
private[velox] class CreateTableResponse