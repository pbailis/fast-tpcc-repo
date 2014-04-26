package edu.berkeley.velox.trigger.server

import scala.collection.immutable.HashMap
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.catalog.Catalog
import edu.berkeley.velox.trigger._
import edu.berkeley.velox.operations.internal.InsertionRequest
import edu.berkeley.velox.util.NonThreadedExecutionContext.context
import scala.concurrent.Future

// Inserts new table rows into the table's indexes.
class IndexUpdateTrigger(val dbName: String, val tableName: String) extends AfterInsertAsyncRowTrigger with AfterDeleteAsyncRowTrigger with AfterUpdateAsyncRowTrigger with Logging {
  // Info for helping update an index.
  private case class IndexInfo(name: String, schema: Schema, rowProjection: Seq[Int])
  private var indexes = new HashMap[String, IndexInfo]

  override def initialize(dbName: String, tableName: String) {
    val tableSchema = Catalog.getSchema(this.dbName, this.tableName)

    tableSchema.indexes.keys.foreach(indexName => {
      // Find all indexes and save information for updating them.
      val fullIdxName = this.tableName + "." + indexName
      val idxSchema = Catalog.getSchema(this.dbName, fullIdxName)
      // if idxSchema is null, the index has not be added to the catalog yet.
      if (idxSchema != null) {
        val rowProjection = idxSchema.columns.map(col => {
          tableSchema.indexOf(col)
        })
        indexes += ((fullIdxName, IndexInfo(fullIdxName, idxSchema, rowProjection)))
      }
    })
  }

  override def afterInsertAsync(ctx: TriggerContext, inserted: Seq[Row]): Future[Any] = {
    val allFutures = inserted.flatMap(row => {
      val futures = indexes.values.map(idxInfo => {
        // Update all the indexes.
        val indexRow = row.project(idxInfo.rowProjection)
        val insertSet = new InsertSet
        insertSet.appendRow(indexRow)
        val pkey = Catalog.extractPrimaryKey(dbName, idxInfo.name, indexRow)
        val partition = ctx.partitioner.getMasterPartition(pkey)
        ctx.messageService.send(partition, new InsertionRequest(dbName, idxInfo.name, insertSet))
      })
      futures
    })
    Future.sequence(allFutures)
  }

  override def afterDeleteAsync(ctx: TriggerContext, deleted: Seq[Row]): Future[Any] = {
    Future.successful()
  }

  override def afterUpdateAsync(ctx: TriggerContext, updated: Seq[(Row, Row)]): Future[Any] = {
    Future.successful()
  }
}
