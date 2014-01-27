package edu.berkeley

import com.codahale.metrics.MetricRegistry

package object velox {
  type NetworkDestinationHandle = Int
  type RequestId = Long
  val metrics = new MetricRegistry()

  case class Key(val k: Int) { override def toString() = k.toString}
  case class Value(val value: String) { override def toString() = value}
}

