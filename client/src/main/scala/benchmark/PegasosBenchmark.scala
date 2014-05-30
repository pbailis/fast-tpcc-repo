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
  val DATA_SIZE_PER_BOX = 10000
  val DATA_DIMENSION = 5

  var LOCAL_EVALUATION_PERIOD_MS = 20

  val GAMMA = 1.0
  val NUM_ITERATIONS = DATA_SIZE_PER_BOX*2

  def main(args: Array[String]) {
    VeloxConfig.initialize(args)
    val client = new VeloxConnection

    val parser = new scopt.OptionParser[Unit]("pegasos") {
      opt[Int]("log_ms") foreach {
        i => LOCAL_EVALUATION_PERIOD_MS = i
      }
    }

    parser.parse(args)

    val realModel = GeneralizedLinearModels.randomModel(DATA_DIMENSION)

    val loadFutures = client.ms.sendAllRemote(new LoadExamples(realModel, DATA_DIMENSION))

    logger.info(s"Waiting to load examples...")
    Await.ready(Future.sequence(loadFutures), Duration.Inf)
    logger.info(s"Examples loaded!")

    logger.error(s"Starting run!")
    val runFutures = client.ms.sendAllRemote(new RunPegasosAsync(GAMMA, NUM_ITERATIONS, LOCAL_EVALUATION_PERIOD_MS))
    val doneF = Future.sequence(runFutures)
    Await.ready(doneF, Duration.Inf)
    logger.error(s"Finished run!")

    val results = doneF.value.get.get

    println(s"real model is $realModel")
    results.foreach( r => println(s"${r.finalModel.model}, ${r.finalModel.localLoss}"))

    val computedModel = results.foldLeft(new DoubleVector(DATA_DIMENSION)){ (acc, r) => acc + r.finalModel.model }/results.size

    val totalLoss = results.map{ r => r.finalModel.localLoss }.sum + Math.pow(computedModel.l2norm(), 2)*GAMMA
    println(s"computed model is $computedModel, total loss is $totalLoss")

    val numSamplesToConsider = results.map { r => r.seriesModels.size }.min
    for(i <- 0 until numSamplesToConsider) {
      val time = (i+1)*LOCAL_EVALUATION_PERIOD_MS
      val currentModel = results.foldLeft(new DoubleVector(DATA_DIMENSION)){ (acc, r) => acc + r.seriesModels(i).model }/results.size
      val currentLoss = results.map{ r => r.seriesModels(i).localLoss }.sum + Math.pow(currentModel.l2norm(), 2)*GAMMA
      println(s"at time ${time}ms, global loss was $currentLoss")
    }
  }
}
