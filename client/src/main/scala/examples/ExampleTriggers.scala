package edu.berkeley.velox.examples

import com.typesafe.scalalogging.slf4j.Logging
import java.net.InetSocketAddress
import scala.concurrent.Future
import edu.berkeley.velox.trigger._
import edu.berkeley.velox.frontend.VeloxConnection
import edu.berkeley.velox.frontend.api.Database
import edu.berkeley.velox.datamodel.Row

class MyTrigger extends AfterDeleteRowTrigger with AfterInsertRowTrigger with AfterUpdateRowTrigger with Logging {
  override def initialize(dbName: String, tableName: String) {
  }

  override def afterDelete(ctx: TriggerContext, deleted: Seq[Row]): Future[Any] = {
    Future.successful()
  }

  override def afterInsert(ctx: TriggerContext, inserted: Seq[Row]): Future[Any] = {
    Future.successful()
  }

  override def afterUpdate(ctx: TriggerContext, updated: Seq[(Row, Row)]): Future[Any] = {
    Future.successful()
  }
}

object ExampleTriggers extends Logging {
  def main(args: Array[String]) {
    logger.info("example triggers")
    val conn = new VeloxConnection()
    val db : Database = conn.database("db")
    db.registerTrigger("table", classOf[MyTrigger])
  }
}
