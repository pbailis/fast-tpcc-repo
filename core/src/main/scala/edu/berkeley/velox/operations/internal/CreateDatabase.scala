package edu.berkeley.velox.operations.internal

import edu.berkeley.velox.datamodel.DatabaseName
import edu.berkeley.velox.rpc.Request

private[velox] case class CreateDatabaseRequest(name: DatabaseName) extends Request[CreateDatabaseResponse]
private[velox] class CreateDatabaseResponse