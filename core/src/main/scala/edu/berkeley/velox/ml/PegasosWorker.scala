package edu.berkeley.velox.ml

import java.util
import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Random
import edu.berkeley.velox.rpc.MessageService

class PegasosWorker(val ms: MessageService) extends Logging {
  var examples = new util.ArrayList[Example]
  var w: Vector = null

  def receive(msg: PegasosMessage): Any = {
    msg match {
      case LoadExamples(e) => {
        examples = e
        true
      }
      case RunPegasosAsync(gamma, iterations) => {
        runPegasosAsync(gamma, iterations)
      }
      case DeltaUpdate(d) => {
        w.add(d)
      }
      case _ => {
        logger.error(s"Got message of unknown type! $msg")
      }
    }
  }

  def runPegasosAsync(gamma: Int, iterations: Int): Vector = {
    w = new Vector(examples.get(0)._1.size)

    for(t <- 0 to iterations) {
      val i_t = Random.nextInt(examples.size()-1)
      val n_t = 1./(gamma*t)

      val x_it = examples.get(i_t)._1
      val y_it = examples.get(i_t)._2

      val prod = y_it*w.dot(x_it)
      var w_next: Vector = null

      if(prod < 1) {
        w_next = w.scale(1-n_t*gamma).add(x_it.scale(n_t*y_it))
      } else {
        w_next = w.scale(1-n_t*gamma)
      }

      val w_delta = w_next.subtract(w)

      ms.sendAll(new DeltaUpdate(w_delta))

      w = w_delta
    }
    w
  }

}
