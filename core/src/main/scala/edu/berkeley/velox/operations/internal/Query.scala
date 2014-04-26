package edu.berkeley.velox.operations.internal

import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.rpc.Request

private[velox] case class QueryRequest(val query: Query) extends Request[QueryResponse]
private[velox] case class QueryResponse(val results: ResultSet) extends AnyVal
