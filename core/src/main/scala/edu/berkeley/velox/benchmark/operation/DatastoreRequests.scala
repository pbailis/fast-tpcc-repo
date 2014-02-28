package edu.berkeley.velox.benchmark.operation

import edu.berkeley.velox.datamodel.{PrimaryKey, Row}
import java.util
import edu.berkeley.velox.rpc.{OneWayRequest, Request}
import edu.berkeley.velox.{RequestId, NetworkDestinationHandle}

case class PreparePutAllRequest(val values: util.Map[PrimaryKey, Row]) extends Request[PreparePutAllResponse]
class PreparePutAllResponse

case class CommitPutAllRequest(val timestamp: Long, val deferredIncrement: DeferredIncrement = null) extends Request[CommitPutAllResponse]
class CommitPutAllResponse(val incrementResponse: Int = -1)

case class GetAllRequest(val keys:Array[PrimaryKey], val values: Array[Row]) extends Request[GetAllResponse]
case class GetAllResponse(val values: util.Map[PrimaryKey, Row])

case class SerializableGetAllRequest(val keys: util.Map[PrimaryKey, Row]) extends Request[SerializableGetAllResponse]
case class SerializableGetAllResponse(val values: util.Map[PrimaryKey, Row])

case class SerializablePutAllRequest(val values: util.HashMap[PrimaryKey, Row]) extends Request[SerializablePutAllResponse]
class SerializablePutAllResponse

case class SerializableUnlockRequest(val keys: util.HashSet[PrimaryKey]) extends OneWayRequest

class MicroCfreePut extends Request[Boolean]

class MicroTwoPLPutAndLock extends Request[Boolean]
class MicroTwoPLUnlock extends OneWayRequest

case class MicroOptimizedTwoPL(val clientID: NetworkDestinationHandle, val clientRequestID: RequestId, val numItems: Int, val numRemaining: Int) extends Request[Boolean]





