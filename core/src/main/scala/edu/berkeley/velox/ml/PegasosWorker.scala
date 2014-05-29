package edu.berkeley.velox.ml

import java.util
import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Random
import edu.berkeley.velox.rpc.{Request, MessageHandler, MessageService}
import edu.berkeley.velox.NetworkDestinationHandle

class PegasosWorker(val ms: MessageService) extends Logging {
  var examples: Array[Example] = null
  var w: DoubleVector = null

  val LAMBDA = 1

  def loadExamples(l: LoadExamples) {
    examples = GeneralizedLinearModels.randomData(l.model, l.n, l.obsNoise)
    logger.info("Loaded examples!")
  }

  def deltaUpdate(v: DoubleVector) {
    w += v
  }

  def runPegasosAsync(gamma: Double, iterations: Int): TrainingResult = {
    w = new DoubleVector(examples(0)._1.size())

    for(t <- 1 to iterations) {
      val i = Random.nextInt(examples.length - 1)
      val eta = 1.0 / (gamma*t)

      val (x, y) = examples(i)

      val prod = y * (w dot x)
      var w_next: DoubleVector = null

      if(prod < 1) {
        w_next = w * (1 - eta * gamma) + x * (eta * y)
      } else {
        w_next = w * (1-eta*gamma)
      }

      val w_delta = w_next - w

      ms.sendAll(new DeltaUpdate(w_delta))

      w = w_next
      logger.info(s"$t, ${GeneralizedLinearModels.hingeLossDataLikelihood(w, examples, LAMBDA)}")

    }

    val loss = GeneralizedLinearModels.hingeLossDataLikelihood(w, examples, LAMBDA)

    new TrainingResult(w, loss)
  }

}
