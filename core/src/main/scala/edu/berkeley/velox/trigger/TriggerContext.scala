package edu.berkeley.velox.trigger

import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.rpc.MessageService
import edu.berkeley.velox.cluster.Partitioner

// Context for defining and executing triggers.
class TriggerContext(val dbName: DatabaseName, val tableName: TableName, val messageService: MessageService, val partitioner: Partitioner) {
}
