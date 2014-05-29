package benchmark

import edu.berkeley.velox.frontend.VeloxConnection
import edu.berkeley.velox.ml._
import scala.util.Random
import scala.concurrent.{Await, Future}
import java.util
import scala.concurrent.duration.Duration
import scala.collection.JavaConversions._
import scala.collection.mutable
import edu.berkeley.velox.util.NonThreadedExecutionContext.context
import edu.berkeley.velox.conf.VeloxConfig
import com.typesafe.scalalogging.log4j.Logging
import edu.berkeley.velox.ml.LoadExamples
import edu.berkeley.velox.ml.RunPegasosAsync

object PegasosBenchmark extends Logging {
  val DATA_SIZE_PER_BOX = 100
  val DATA_DIMENSION = 5

  val GAMMA = 5
  val NUM_ITERATIONS = 10

  def main(args: Array[String]) {
    VeloxConfig.initialize(args)
    val client = new VeloxConnection

    val realModel = GeneralizedLinearModels.randomModel(DATA_DIMENSION)

    val loadFutures = client.ms.sendAllRemote(new LoadExamples(realModel, DATA_DIMENSION))

    logger.info(s"Waiting to load examples...")
    Await.ready(Future.sequence(loadFutures), Duration.Inf)
    logger.info(s"Examples loaded!")

    logger.error(s"Starting run!")
    val runFutures = client.ms.sendAllRemote(new RunPegasosAsync(GAMMA, NUM_ITERATIONS))
    logger.error(s"Finished run!")

    val doneF = Future.sequence(runFutures)
    Await.ready(doneF, Duration.Inf)

    println(s"real model is $realModel")
    doneF.value.get.get.foreach( r => println(s"${r.model}, ${r.localLoss}"))


    //TODO: AVERAGE HERE
  }
}
