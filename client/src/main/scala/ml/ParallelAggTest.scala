package ml

import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.frontend.VeloxConnection

import edu.berkeley.velox.storage.StorageManager
import edu.berkeley.velox.datamodel.{Schema, ResultSet, Query}

import edu.berkeley.velox.datamodel.DataModelConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object ParallelAggTest {
  val dbname = "testDB"
  val tablename = "testTable"
  val testCol = "col"
  val idCol = "id"

  def main(args: Array[String]) {
    VeloxConfig.initialize(args)
    val conn = new VeloxConnection

    val dbf = conn.createDatabase(dbname)
    Await.ready(dbf, Duration.Inf)

    val tf = conn.database(dbname).createTable(tablename, Schema.columns(idCol PRIMARY() INT,
                                                                         testCol INT()))
    Await.ready(tf, Duration.Inf)

    val insertQuery = conn.database(dbname).table(tablename).insertBatch
    for(i <- 0 to 100) {
      insertQuery.insert(idCol -> i, testCol -> i)
    }
    insertQuery.execute()

    val parallelAggregator = (sm: StorageManager) => {
      val rs = sm.database(dbname).table(tablename).select(testCol).executeBlocking()

      var sum = 0
      while(rs.next()) {
        sum += rs.getInt(0)
      }
      sum
    }

    val sumFuture = conn.perPartitionUDF(parallelAggregator)
    Await.ready(sumFuture, Duration.Inf);

    sumFuture.value.get match {
      case Success(x) => {
        println(s"summation is ${x.asInstanceOf[Seq[Int]].reduce(_ + _)}")
      }
      case Failure(t) => {
        println(s"Failed! $t")
      }
    }
  }
}
