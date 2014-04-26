package edu.berkeley.velox.operations.internal

import edu.berkeley.velox.datamodel.{DatabaseName, TableName}
import edu.berkeley.velox.rpc.Request

private[velox] case class RegisterTriggerRequest(dbName: DatabaseName, tableName: TableName, triggerClassName: String, triggerClassBytes: Array[Byte]) extends Request[RegisterTriggerResponse]
private[velox] class RegisterTriggerResponse