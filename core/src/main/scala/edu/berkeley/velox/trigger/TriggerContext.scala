package edu.berkeley.velox.trigger

import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.rpc.MessageService

// Context for defining and executing triggers.
class TriggerContext(val dbName: DatabaseName, val tableName: TableName, val messageService: MessageService) {
}
