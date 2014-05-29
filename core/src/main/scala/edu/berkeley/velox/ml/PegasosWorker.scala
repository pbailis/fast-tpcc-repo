package edu.berkeley.velox.ml

import java.util
import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Random
import edu.berkeley.velox.rpc.MessageService

class PegasosWorker(val ms: MessageService) extends Logging {
  var examples: Array[Example] = null
  var w: DoubleVector = null

  def receive[T](msg: PegasosMessage[T]): Any = {
    msg match {
      case LoadExamples(e) => {
        examples = e
        true
      }
      case RunPegasosAsync(gamma, iterations) => {
        runPegasosAsync(gamma, iterations)
      }
      case DeltaUpdate(d) => {
        w + d
      }
      case _ =>
        logger.error(s"Got message of unknown type! $msg")
    }
  }

  def runPegasosAsync(gamma: Double, iterations: Int): DoubleVector = {
    w = new DoubleVector(examples(0)._1.size() )

    for(t <- 0 to iterations) {
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

      w = w_delta
    }
    w
  }

}
