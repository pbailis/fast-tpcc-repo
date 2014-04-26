package edu.berkeley.velox.trigger

import edu.berkeley.velox.datamodel._
import scala.concurrent.Future

// Trigger calls are not thread-safe, so internal synchronization may be necessary.
// After triggers can be synchronous or asynchronous.
// Synchronous triggers:
//   - guaranteed to finish before responding back to client
// Asynchronous triggers:
//   - NOT guaranteed to finish before responding back to client
// All after triggers return a future of [remaining] computation.

// Base row trigger
sealed trait RowTrigger {
  // Triggers can initialize some state.
  def initialize(dbName: String, tableName: String) {}
}

// TODO: do before triggers get to change the row?

// insert triggers
trait BeforeInsertRowTrigger extends RowTrigger {
  def beforeInsert(ctx: TriggerContext, toInsert: Seq[Row])
}
trait AfterInsertRowTrigger extends RowTrigger {
  def afterInsert(ctx: TriggerContext, inserted: Seq[Row]): Future[Any]
}
trait AfterInsertAsyncRowTrigger extends RowTrigger {
  def afterInsertAsync(ctx: TriggerContext, inserted: Seq[Row]): Future[Any]
}

// delete triggers
trait BeforeDeleteRowTrigger extends RowTrigger {
  def beforeDelete(ctx: TriggerContext, toDelete: Seq[Row])
}
trait AfterDeleteRowTrigger extends RowTrigger {
  def afterDelete(ctx: TriggerContext, deleted: Seq[Row]): Future[Any]
}
trait AfterDeleteAsyncRowTrigger extends RowTrigger {
  def afterDeleteAsync(ctx: TriggerContext, deleted: Seq[Row]): Future[Any]
}

// update triggers
// The tuple is (oldRow, newRow)
trait BeforeUpdateRowTrigger extends RowTrigger {
  def beforeUpdate(ctx: TriggerContext, toUpdate: Seq[(Row, Row)])
}
trait AfterUpdateRowTrigger extends RowTrigger {
  def afterUpdate(ctx: TriggerContext, updated: Seq[(Row, Row)]): Future[Any]
}
trait AfterUpdateAsyncRowTrigger extends RowTrigger {
  def afterUpdateAsync(ctx: TriggerContext, updated: Seq[(Row, Row)]): Future[Any]
}
