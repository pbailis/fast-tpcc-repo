package edu.berkeley.velox.examples

import com.typesafe.scalalogging.slf4j.Logging
import java.net.InetSocketAddress
import edu.berkeley.velox.trigger._
import edu.berkeley.velox.frontend.VeloxConnection
import edu.berkeley.velox.datamodel.Row
import edu.berkeley.velox.operations.commands.QueryDatabase

class MyTrigger extends AfterDeleteRowTrigger with AfterInsertRowTrigger with AfterUpdateRowTrigger with Logging {
  override def initialize(dbName: String, tableName: String) {
  }

  override def afterDelete(ctx: TriggerContext, deleted: Row) {
  }

  override def afterInsert(ctx: TriggerContext, inserted: Row) {
  }

  override def afterUpdate(ctx: TriggerContext, oldRow: Row, newRow: Row) {
  }
}

object ExampleTriggers extends Logging {
  def main(args: Array[String]) {
    logger.info("example triggers")
    implicit val conn = new VeloxConnection()
    val db : QueryDatabase = conn.database("db")
    db.registerTrigger("table", classOf[MyTrigger])
  }
}
