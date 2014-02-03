package edu.berkeley.velox.operations.database.response

import edu.berkeley.velox.datamodel.ResultSet


case class QueryResponse(val results: ResultSet) extends AnyVal
