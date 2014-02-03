package edu.berkeley.velox.datamodel

import edu.berkeley.velox.frontend.VeloxConnection
import java.net.InetSocketAddress
import edu.berkeley.velox.frontend.api.Database
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import edu.berkeley.velox.datamodel.DataModelConverters._

object ExampleDataModel {
  def main(args: Array[String]) {
    val conn = new VeloxConnection(Array(new InetSocketAddress("localhost", 1234)))

    val db : Database = conn.database("peter-db")

    // use existing table
    val table = db.table("test_table")

    // or table insert(...) execute()
    conn insert("id"-> 5, "name" -> "peter") insert ("id" -> 6, "name" -> "lanham") into table execute()

    // or table select (..) where (..) execute()
    val resultSet = Await.result(conn select("id","name") from table where("id" === 5) execute(), Duration.Inf)

    assert(resultSet.size == 1)
    assert(resultSet.getInt("id") == 5)

    val nickTable = db.table("nick-table")

    val newTable = db.createTable("new-table",
                                  Schema.pkey("id").columns("id" -> INTEGER_TYPE, "name" -> STRING_TYPE))
  }
}
