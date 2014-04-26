package edu.berkeley.velox.datamodel

import edu.berkeley.velox.operations.commands.{Operation, QueryOperation}

trait Selectable {
  // Query creation/parsing/validation
  def select(names: ColumnLabel*) : Operation = {
    new QueryOperation(null, names)
  }
}
