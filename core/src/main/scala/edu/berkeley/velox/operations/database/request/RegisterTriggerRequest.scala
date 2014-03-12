package edu.berkeley.velox.operations.database.request

import edu.berkeley.velox.datamodel.{DatabaseName, TableName}
import edu.berkeley.velox.rpc.Request
import edu.berkeley.velox.operations.database.response.RegisterTriggerResponse

case class RegisterTriggerRequest(dbName: DatabaseName, tableName: TableName, triggerClassName: String, triggerClassBytes: Array[Byte]) extends Request[RegisterTriggerResponse]
