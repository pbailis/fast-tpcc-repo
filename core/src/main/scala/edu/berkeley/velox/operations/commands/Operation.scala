package edu.berkeley.velox.operations.commands

import scala.concurrent.Future
import edu.berkeley.velox.datamodel.ResultSet
import edu.berkeley.velox.operations.CommandExecutor

trait Operation {
  def execute() : Future[ResultSet]
  def executeBlocking() : ResultSet
}
