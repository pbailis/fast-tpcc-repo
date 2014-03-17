package edu.berkeley.velox

import org.scalatest.FunSuite
import edu.berkeley.velox.datamodel.{Row, ResultSet}
import edu.berkeley.velox.datamodel.DataModelConverters._


class ResultSetSuite extends FunSuite {
  test("basic result set insertion") {
    val rows = new Array[Row](2)
    rows(0) = (new Row(1)).set(0, 0)
    rows(1) = (new Row(1)).set(0, 1)

    val rs = new ResultSet(rows)
    assert(rs.size == 2)
    assert(rs.next == true)
    assert(rs.getInt(0) == 0)
    assert(rs.next == true)
    assert(rs.getInt(0) == 1)
    assert(!rs.hasNext)
  }

}
