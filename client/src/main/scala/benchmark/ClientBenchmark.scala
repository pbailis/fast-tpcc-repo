package edu.berkeley.velox.benchmark

import edu.berkeley.velox.conf.VeloxConfig
import java.util.concurrent.atomic.AtomicInteger
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.datamodel.Schema
import edu.berkeley.velox.datamodel.DataModelConverters._
import scala.util.Random
import edu.berkeley.velox.frontend.VeloxConnection
import java.net.InetSocketAddress
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.catalog.Catalog

// this causes our futures to not thread
import edu.berkeley.velox.util.NonThreadedExecutionContext.context

import scala.util.{Success, Failure}
import java.util.UUID

import scala.language.postfixOps

import edu.berkeley.velox.trigger._
import edu.berkeley.velox.operations.database.request.InsertionRequest

// Inserts new table rows into the index.
class IndexTrigger extends AfterInsertRowTrigger with Logging {
  var idxSchema: Schema = null
  var rowProjection: Seq[Int] = null
  var indexName = ""

  override def initialize(dbName: String, tableName: String) {
    val tableSchema = Catalog.getSchema(dbName, tableName)
    // hard-coded index name for now.
    indexName = tableName + ".index1"
    idxSchema = Catalog.getSchema(dbName, indexName)
    // projection indices to map from table row to index row.
    rowProjection = idxSchema.columns.map(col => {
      tableSchema.indexOf(col)
    })
  }

  override def afterInsert(ctx: TriggerContext, inserted: Row) {
    val indexRow = inserted.project(rowProjection)
    val insertSet = new InsertSet
    insertSet.appendRow(indexRow)
    val pkey = Catalog.extractPrimaryKey(ctx.dbName, indexName, indexRow)
    val partition = ctx.partitioner.getMasterPartition(pkey)
    ctx.messageService.send(partition, new InsertionRequest(ctx.dbName, indexName, insertSet))
  }
}

object ClientBenchmark extends Logging {

  def main(args: Array[String]) {
    VeloxConfig.initialize(args)
    val numPuts = new AtomicInteger()
    val numGets = new AtomicInteger()

    val nanospersec = math.pow(10, 9)

    val keyrange = 10000
    var parallelism = 64
    var numops = 100000
    var waitTimeSeconds = 20
    var pctReads = 0.5
    var status_time = 10
    var run = false
    var load = false

    var useFutures = true
    var computeLatency = false
    var testIndex = false

    val parser = new scopt.OptionParser[Unit]("velox") {
      opt[Int]("timeout") foreach {
        i => waitTimeSeconds = i
      } text ("Time (s) for benchmark")
      opt[Int]("ops") foreach {
        i => numops = i
      } text ("Ops in the benchmark")
      opt[Int]("parallelism") foreach {
        i => parallelism = i
      } text ("Parallelism in thread pool")
      opt[Double]("pct_reads") foreach {
        i => pctReads = i
      } text ("Percentage read vs. write operations")
      opt[Int]("status_time") foreach {
        i => status_time = i
      }
      opt[Int]('b', "buffer_size") foreach {
        p => VeloxConfig.bufferSize = p
      } text("Size (in bytes) to make the network buffer")
      opt[Int]("sweep_time") foreach {
        p => VeloxConfig.sweepTime = p
      } text("Time the ArrayNetworkService send sweep thread should wait between sweeps")
      opt[Boolean]("latency") foreach {
        i => computeLatency = i
      } text ("Compute average latency of each request")
      opt[String]("network_service") foreach {
        i => VeloxConfig.networkService = i
      } text ("Which network service to use [nio/array]")
      opt[Boolean]("test_index") foreach {
        i => testIndex = i
      } text ("Test simple index inserts using triggers. (experimental)")
      opt[Unit]("run") foreach {
        i => run = true
      } text ("run")
      opt[Unit]("load") foreach {
        i => load = true
      } text ("laod")
    }

    val opsSent = new AtomicInteger(0)
    val opsDone = new AtomicInteger(0)

    def opDone() {
      val o = opsDone.incrementAndGet
      if (o == numops) {
        opsDone.synchronized {
          opsDone.notify()
        }
      }
    }

    parser.parse(args)

    // (num reqs, avg)
    var currentLatency = (0, 0.0)

    def updateLatency(runtime: Long) = synchronized {
      val ttl = currentLatency._1 * currentLatency._2
      val nc = currentLatency._1 + 1
      currentLatency = (nc,(ttl+runtime)/nc)
    }

    val ostart = System.nanoTime
    val client = new VeloxConnection
    val DB_NAME = "client-benchmark-db"
    val TABLE_NAME = "table1"
    val ID_COL = "id"
    val STR_COL = "str"

    if (load) {

      val dbf = client.createDatabase(DB_NAME)
      Await.ready(dbf, Duration.Inf)
      logger.info("database successfully added")

      val db = dbf.value.get.get

      val tblf = db.createTable(TABLE_NAME,
        Schema.columns(ID_COL PRIMARY() INT,
          STR_COL STRING))
      Await.ready(tblf, Duration.Inf)
      logger.info("table successfully added")

      if (testIndex) {
        // create the index, which is another table for now.
        val idxf = db.createTable(TABLE_NAME + ".index1",
                                  Schema.columns(STR_COL PRIMARY() STRING, ID_COL PRIMARY() INT))
        Await.ready(idxf, Duration.Inf)
        logger.info("index successfully added")

        // add the index insert trigger on the table.
        db.registerTrigger(TABLE_NAME, classOf[IndexTrigger])
      }

      val table = tblf.value.get.get
      if (!run)
        System.exit(0)
    }

   if (!run) {
     logger.error("RUN FALSE")
     System.exit(-1)
   }

    logger.error(s"local dbs: ${Catalog.listLocalDatabases}")
    logger.error(s"local tables: ${Catalog.listLocalTables(DB_NAME)}")


    val table = client.database(DB_NAME).table(TABLE_NAME)
    logger.info(s"Starting $parallelism threads!")
    for (i <- 0 to parallelism) {
    new Thread(new Runnable {
      val rand = new Random
      override def run() = {
        while (opsSent.get < numops) {
          if (rand.nextDouble() < pctReads) {
            val f = (client select STR_COL from table where ID_COL ==* rand.nextInt(keyrange)).execute
            val startTime =
              if (computeLatency) System.nanoTime
              else 0
            f onComplete {
              case Success(value) => {
                if (computeLatency)
                  updateLatency(System.nanoTime-startTime)
                numGets.incrementAndGet()
                opDone
              }
              case Failure(t) => logger.info("An error has occured: " + t.getMessage)
            }
          } else {
            val key = rand.nextInt(keyrange)
            val f = (client insert (ID_COL->key, STR_COL->key.toString) into table).execute

            val startTime =
              if (computeLatency) System.nanoTime
              else 0
            f onComplete {
              case Success(value) => {
                if (computeLatency)
                  updateLatency(System.nanoTime-startTime)
                numPuts.incrementAndGet()
                opDone
              }
              case Failure(t) => logger.info("An error has occured: " + t.getMessage)
            }
          }
          opsSent.incrementAndGet
        }
      }
      logger.info("Thread is done sending")
    }).start
  }


    if(status_time > 0) {
      new Thread(new Runnable {
        override def run() {
          while(opsDone.get() < numops) {
            Thread.sleep(status_time*1000)
            val curTime = (System.nanoTime-ostart).toDouble/nanospersec
            val curThru = (numGets.get()+numPuts.get()).toDouble/curTime
            logger.info(s"STATUS @ ${curTime}s: $curThru ops/sec ($opsDone ops done)")
          }
        }
      }).start
    }

    opsDone.synchronized {
      opsDone.wait(waitTimeSeconds * 1000)
    }

    val gstop = System.nanoTime
    val gtime = (gstop - ostart) / nanospersec

    val nPuts = numPuts.get()
    val nGets = numGets.get()

    val pthruput = nPuts.toDouble / gtime.toDouble
    val gthruput = nGets.toDouble / gtime.toDouble
    val totthruput = (nPuts + nGets).toDouble / gtime.toDouble
    logger.info(s"In $gtime seconds and with $parallelism threads, completed $numPuts PUTs ($pthruput ops/sec), $numGets GETs ($gthruput ops/sec)\nTOTAL THROUGHPUT: $totthruput ops/sec")
    if (computeLatency)
      logger.info(s"Average latency ${currentLatency._2} milliseconds")

    if (testIndex) {
      // read some entries in the index.
      val tableIdx = client.database(DB_NAME).table(TABLE_NAME + ".index1")
      val f = (client select (STR_COL, ID_COL) from tableIdx where ID_COL <* 123).execute
      f onComplete {
        case Success(rset) => {
          rset.beforeFirst()
          logger.info("result.size: " + rset.size())
          while (rset.next()) {
            logger.info("result: (" + rset.getString(0) + ", " + rset.getInt(1) + ")")
          }
        }
      }
      Await.result(f, Duration.Inf)
      Thread.sleep(5*1000)
    }

    System.exit(0)
  }

  def nextKey(rand: Random, keyrange: Int): String = {
    rand.nextInt(keyrange).toString
  }
}
