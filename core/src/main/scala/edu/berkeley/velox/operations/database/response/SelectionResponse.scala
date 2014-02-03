package edu.berkeley.velox.operations.database.response

import edu.berkeley.velox.datamodel.ResultSet


case class SelectionResponse(val results: ResultSet) extends AnyVal
