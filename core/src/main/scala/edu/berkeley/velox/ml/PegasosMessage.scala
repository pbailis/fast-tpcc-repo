package edu.berkeley.velox.ml

import edu.berkeley.velox.rpc.{OneWayRequest, Request}

trait PegasosMessage 


case class LoadExamples(examples: Array[Example]) extends PegasosMessage with Request[Boolean]
case class RunPegasosAsync(gamma: Int, numIterations: Int) extends PegasosMessage with Request[DoubleVector]
case class DeltaUpdate(delta: DoubleVector) extends PegasosMessage with OneWayRequest