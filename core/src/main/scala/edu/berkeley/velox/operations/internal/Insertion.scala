package edu.berkeley.velox.operations.internal

import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.rpc.Request

private[velox] case class InsertionRequest(val database: DatabaseName,
                            val table: TableName,
                            val insertSet: InsertSet) extends Request[InsertionResponse]
private[velox] class InsertionResponse