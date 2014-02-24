package edu.berkeley.velox.operations.database.request

import edu.berkeley.velox.datamodel.DatabaseName
import edu.berkeley.velox.rpc.Request
import edu.berkeley.velox.operations.database.response.CreateDatabaseResponse

case class CreateDatabaseRequest(name: DatabaseName) extends Request[CreateDatabaseResponse]