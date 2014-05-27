package edu.berkeley.velox.ml

import edu.berkeley.velox.rpc.{OneWayRequest, Request}

trait PegasosMessage

case class LoadExamples(val examples: Array[Example]) extends PegasosMessage with Request[Boolean]
case class RunPegasosAsync(val gamma: Int, val numIterations: Int) extends PegasosMessage with Request[Vector]

case class DeltaUpdate(val delta: Vector) extends PegasosMessage with OneWayRequest