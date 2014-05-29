package edu.berkeley.velox.ml

import edu.berkeley.velox.rpc.{OneWayRequest, Request}

trait PegasosMessage[T] extends Request[T]

case class LoadExamples(examples: Array[Example]) extends PegasosMessage[Boolean]
case class RunPegasosAsync(gamma: Int, numIterations: Int) extends PegasosMessage[DoubleVector]
case class DeltaUpdate(delta: DoubleVector) extends PegasosMessage[Unit]