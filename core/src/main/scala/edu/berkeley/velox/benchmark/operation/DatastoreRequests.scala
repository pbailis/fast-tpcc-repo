package edu.berkeley.velox.benchmark.operation

import edu.berkeley.velox.datamodel.{DataItem, ItemKey}
import java.util
import edu.berkeley.velox.rpc.Request

case class PreparePutAllRequest(val values: util.HashMap[ItemKey, DataItem]) extends Request[PreparePutAllResponse]
class PreparePutAllResponse

case class CommitPutAllRequest(val timestamp: Long) extends Request[CommitPutAllResponse]
class CommitPutAllResponse

case class GetAllRequest(val keys: util.ArrayList[ItemKey]) extends Request[GetAllResponse]
case class GetAllResponse(val values: util.HashMap[ItemKey, DataItem])

