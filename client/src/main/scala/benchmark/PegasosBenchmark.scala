package benchmark

import edu.berkeley.velox.frontend.VeloxConnection
import edu.berkeley.velox.ml.{RunPegasosAsync, LoadExamples, Example, DoubleVector}
import scala.util.Random
import scala.concurrent.{Await, Future}
import java.util
import scala.concurrent.duration.Duration
import scala.collection.JavaConversions._
import scala.collection.mutable
import edu.berkeley.velox.util.NonThreadedExecutionContext.context
import edu.berkeley.velox.conf.VeloxConfig

object PegasosBenchmark {
  val DATA_SIZE_PER_BOX = 10
  val DATA_DIMENSION = 5

  val GAMMA = 5
  val NUM_ITERATIONS = 1000000

  def main(args: Array[String]) {
    VeloxConfig.initialize(args)
    val client = new VeloxConnection

    val loadFutures = client.ms.getConnections().map(
    conn => {
      val examples = new Array[Example](DATA_SIZE_PER_BOX)

      for (i <- 0 until DATA_SIZE_PER_BOX) {
        val vector = new DoubleVector(DATA_DIMENSION)
        for (j <- 0 until DATA_DIMENSION) {
          vector.arr(j) = Random.nextInt()
        }

        examples(i) = (vector, if (Random.nextBoolean()) 1 else -1)
      }

      client.ms.send(conn, new LoadExamples(examples))
    })

    Await.ready(Future.sequence(loadFutures), Duration.Inf)

    val runFutures = client.ms.sendAll(new RunPegasosAsync(GAMMA, NUM_ITERATIONS))

    Await.ready(Future.sequence(runFutures), Duration.Inf)

    //TODO: AVERAGE HERE
  }
}
