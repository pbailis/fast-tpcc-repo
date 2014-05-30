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
  var DATA_SIZE_PER_BOX = 500
  val DATA_DIMENSION = 100

  var LOCAL_EVALUATION_PERIOD_MS = 20

  val GAMMA = 1.0
  var NUM_ITERATIONS = DATA_SIZE_PER_BOX*100

  var DATA_NOISE = 0.3

  def main(args: Array[String]) {
    VeloxConfig.initialize(args)
    val client = new VeloxConnection

    val parser = new scopt.OptionParser[Unit]("pegasos") {
      opt[Int]("log_ms") foreach {
        i => LOCAL_EVALUATION_PERIOD_MS = i
      }
      opt[Int]("iterations") foreach {
        i => NUM_ITERATIONS = i
      }
      opt[Int]("points_per_worker") foreach {
        i => DATA_SIZE_PER_BOX = i
      }
      opt[Double]("data_noise") foreach {
        i => DATA_NOISE = i
      }
    }

    parser.parse(args)

    val realModel = GeneralizedLinearModels.randomModel(DATA_DIMENSION)

    val loadFutures = client.ms.sendAllRemote(new LoadExamples(realModel, DATA_DIMENSION, DATA_NOISE))

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

    val totalLoss = results.map{ r => r.finalModel.localLoss }.sum + Math.pow(computedModel.l2norm(), 2)/2*GAMMA
    println(s"computed model is $computedModel, total loss is $totalLoss")

    val numSamplesToConsider = results.map { r => r.seriesModels.size }.min
    for(i <- 0 until numSamplesToConsider) {
      val time = (i+1)*LOCAL_EVALUATION_PERIOD_MS
      val currentModel = results.foldLeft(new DoubleVector(DATA_DIMENSION)){ (acc, r) => acc + r.seriesModels(i).model }/results.size
      val currentLoss = results.map{ r => r.seriesModels(i).localLoss }.sum/results.size + Math.pow(currentModel.l2norm(), 2)/2*GAMMA
      println(s"at time ${time}ms, global loss was $currentLoss")
    }

    for((r, id) <- results.zipWithIndex) {
      for (i <- 0 until numSamplesToConsider) {
        val time = (i + 1) * LOCAL_EVALUATION_PERIOD_MS
        val currentModel = r.seriesModels(i).model
        val currentLoss = r.seriesModels(i).localLoss+ Math.pow(currentModel.l2norm(), 2)/2 * GAMMA
        println(s"at time ${time}ms, machine $id loss was $currentLoss")
      }
    }
  }
}
