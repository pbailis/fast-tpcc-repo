package edu.berkeley

import com.codahale.metrics.MetricRegistry

package object velox {
  type PartitionId = Int
  type RequestId = Long
  val metrics = new MetricRegistry()
}
