package edu.berkeley.velox.trigger

import edu.berkeley.velox.datamodel.Row

// Trigger calls are not thread-safe, so internal synchronization may be necessary.
// All triggers are synchronous for now.
// TODO: design asynchronous triggers.

// Base row trigger
sealed trait RowTrigger {
  // Triggers can initialize some state.
  def initialize() {}
}

// TODO: do before triggers get to change the row?

// insert triggers
trait BeforeInsertRowTrigger extends RowTrigger {
  def beforeInsert(ctx: TriggerContext, toInsert: Row)
}
trait AfterInsertRowTrigger extends RowTrigger {
  def afterInsert(ctx: TriggerContext, inserted: Row)
}

// delete triggers
trait BeforeDeleteRowTrigger extends RowTrigger {
  def beforeDelete(ctx: TriggerContext, toDelete: Row)
}
trait AfterDeleteRowTrigger extends RowTrigger {
  def afterDelete(ctx: TriggerContext, deleted: Row)
}

// update triggers
trait BeforeUpdateRowTrigger extends RowTrigger {
  def beforeUpdate(ctx: TriggerContext, oldRow: Row, newRow: Row)
}
trait AfterUpdateRowTrigger extends RowTrigger {
  def afterUpdate(ctx: TriggerContext, oldRow: Row, newRow: Row)
}
