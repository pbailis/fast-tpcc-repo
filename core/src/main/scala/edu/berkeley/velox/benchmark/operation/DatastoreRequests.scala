package edu.berkeley.velox.benchmark.operation

import edu.berkeley.velox.datamodel.{PrimaryKey, Row}
import java.util
import edu.berkeley.velox.rpc.Request

case class PreparePutAllRequest(val values: util.HashMap[PrimaryKey, Row]) extends Request[PreparePutAllResponse]
class PreparePutAllResponse

case class CommitPutAllRequest(val timestamp: Long) extends Request[CommitPutAllResponse]
class CommitPutAllResponse

case class GetAllRequest(val keys: util.HashMap[PrimaryKey, Row]) extends Request[GetAllResponse]
case class GetAllResponse(val values: util.HashMap[PrimaryKey, Row])

