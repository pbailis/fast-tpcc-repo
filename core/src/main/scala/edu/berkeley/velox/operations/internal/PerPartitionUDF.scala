package edu.berkeley.velox.operations.internal

import edu.berkeley.velox.udf.PerPartitionUDF
import edu.berkeley.velox.rpc.Request

private[velox] class PerPartitionUDFRequest(val udf: PerPartitionUDF) extends Request[Any]