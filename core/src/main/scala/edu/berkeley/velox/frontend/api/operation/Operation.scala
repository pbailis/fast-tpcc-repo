package edu.berkeley.velox.datamodel.api.operation

import scala.concurrent.Future
import edu.berkeley.velox.datamodel.ResultSet

trait Operation {
  def execute() : Future[ResultSet]
}
