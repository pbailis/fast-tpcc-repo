package edu.berkeley.velox.ml

import java.util
import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Random
import edu.berkeley.velox.rpc.{Request, MessageHandler, MessageService}
import edu.berkeley.velox.NetworkDestinationHandle
import java.util.concurrent.{TimeUnit, Executors}
import java.util.concurrent.atomic.AtomicBoolean

class PegasosWorker(val ms: MessageService) extends Logging {
  var examples: Array[Example] = null
  var w: DoubleVector = null

  val loggedModels = new util.Vector[DoubleVector]()

  val LAMBDA = 1.0

  def loadExamples(l: LoadExamples) {
    examples = GeneralizedLinearModels.randomData(l.model, l.n, l.obsNoise)
    logger.info("Loaded examples!")
  }

  def deltaUpdate(v: DoubleVector) {
    w += v
  }

  def runPegasosAsync(msg: RunPegasosAsync): PegasosReturn = {
    val gamma = msg.gamma
    val iterations = msg.numIterations

    val done = new AtomicBoolean()

    w = new DoubleVector(examples(0)._1.size())

    if(msg.localPeriodEvaluationMs > 0) {
      val modelLogExecutor = Executors.newScheduledThreadPool(1)
      modelLogExecutor.schedule(new Runnable {
        override def run() = {
          if(!done.get()) {
            loggedModels.add(w)
            modelLogExecutor.schedule(this, msg.localPeriodEvaluationMs, TimeUnit.MILLISECONDS)
          }
        }
      }, msg.localPeriodEvaluationMs, TimeUnit.MILLISECONDS)
    }

    for(t <- 1 to iterations) {
      val i = Random.nextInt(examples.length - 1)
      val eta = 1.0 / (gamma*t)

      val (x, y) = examples(i)

      val prod = y * (w dot x)

      val w_next = if(prod < 1) {
        w * (1 - eta * gamma) + x * (eta * y)
      } else {
        w * (1-eta*gamma)
      }

      val w_delta = w_next - w

      ms.sendAll(new DeltaUpdate(w_delta))

      w = w_next
    }

    done.set(true)

    val loss = GeneralizedLinearModels.hingeLossDataLikelihoodLocal(w, examples, LAMBDA)

    val historicalResults = if(msg.localPeriodEvaluationMs > 0) {
      val numSamples = loggedModels.size()
      val ret = new Array[TrainingResult](numSamples)

      for(i <- 0 until numSamples) {
        val model = loggedModels.get(i)
        ret(i) = new TrainingResult(model, GeneralizedLinearModels.hingeLossDataLikelihoodLocal(model, examples, LAMBDA))
      }
      ret
    } else {
      new Array[TrainingResult](0)
    }

    new PegasosReturn(new TrainingResult(w, loss), historicalResults)
  }

}
