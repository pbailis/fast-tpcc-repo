package edu.berkeley

import com.codahale.metrics.MetricRegistry
import java.util.concurrent.ConcurrentHashMap
import edu.berkeley.velox.datamodel.{Key, Value}

package object velox {
  type NetworkDestinationHandle = Int
  type RequestId = Long
  type TableInstance = ConcurrentHashMap[Key, Value]
  val metrics = new MetricRegistry()
}

