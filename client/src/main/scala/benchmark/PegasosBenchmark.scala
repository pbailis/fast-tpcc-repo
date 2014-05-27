package benchmark

import edu.berkeley.velox.frontend.VeloxConnection
import edu.berkeley.velox.ml.{RunPegasosAsync, LoadExamples, Example, Vector}
import scala.util.Random
import scala.concurrent.{Await, Future}
import java.util
import scala.concurrent.duration.Duration

class PegasosBenchmark {
  val client = new VeloxConnection

  val DATA_SIZE_PER_BOX = 10
  val DATA_DIMENSION = 5

  val GAMMA = 5
  val NUM_ITERATIONS = 1000000

  val loadFutures = new util.ArrayList[Future[Boolean]]()

  for(dest <- client.ms.getConnections) {
    val examples = new Array[Example](DATA_SIZE_PER_BOX)

    for(i <- 0 until DATA_SIZE_PER_BOX) {
      val vector = new Vector(DATA_DIMENSION)
      for(j <- 0 until DATA_DIMENSION) {
        vector.arr(j) = Random.nextInt()
      }

      examples(i) = (vector, if(Random.nextBoolean()) 1 else -1)
    }

    loadFutures.add(client.ms.send(dest, new LoadExamples(examples)))
  }

  Await.ready(Future.sequence(loadFutures), Duration.Inf)

  val runFutures = client.ms.sendAll(new RunPegasosAsync(GAMMA, NUM_ITERATIONS))

  Await.ready(Future.sequence(runFutures), Duration.Inf)

  //TODO: AVERAGE HERE
}
