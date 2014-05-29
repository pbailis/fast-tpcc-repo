package edu.berkeley.velox.ml

import edu.berkeley.velox.rpc.{MessageHandler, OneWayRequest, Request}
import edu.berkeley.velox.NetworkDestinationHandle
import scala.concurrent._
import edu.berkeley.velox.util.NonThreadedExecutionContext.context

case class TrainingResult(model: DoubleVector, localLoss: Double)

case class LoadExamples(model: DoubleVector, n: Int, obsNoise: Double = 0.3) extends Request[Boolean]
case class RunPegasosAsync(gamma: Double, numIterations: Int) extends Request[TrainingResult]
case class DeltaUpdate(delta: DoubleVector) extends OneWayRequest

class PegasosLoadExamplesHandler(val w: PegasosWorker) extends MessageHandler[Boolean, LoadExamples] {
  def receive(src: NetworkDestinationHandle, msg: LoadExamples) = {
    future {
      w.loadExamples(msg)
      true
    }
  }
}

class PegasosRunAsyncHandler(val w: PegasosWorker) extends MessageHandler[TrainingResult, RunPegasosAsync] {
  def receive(src: NetworkDestinationHandle, msg: RunPegasosAsync) = {
    future {
      w.runPegasosAsync(msg.gamma, msg.numIterations)
    }
  }
}

class PegasosDeltaUpdateHandler(val w: PegasosWorker) extends MessageHandler[Unit, DeltaUpdate] {
  def receive(src: NetworkDestinationHandle, msg: DeltaUpdate) = {
    future {
      w.deltaUpdate(msg.delta)
    }
  }
}
