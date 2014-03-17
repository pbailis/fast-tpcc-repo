package edu.berkeley.velox

import edu.berkeley.velox.catalog.Catalog
import org.scalatest.FunSuite
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.datamodel.DataModelConverters._
import edu.berkeley.velox.storage.SkipListStorageManager

class SkipListStorageSuite extends FunSuite {

  val schema = Schema.columns(new IntColumn("a",true),
                              new IntColumn("b",true),
                              new IntColumn("c",false),
                              new IntColumn("d",false))

  Catalog._createDatabaseTrigger("testdb")
  Catalog._createTableTrigger("testdb","testtable",schema)

  val m = new SkipListStorageManager
  Catalog.registerStorageManager(m,true)

  test("Insert into storage") {
    val is = new InsertSet
    var i = 0
    while (i < 20) {
      is.newRow(4)
      if (i < 10)
        is.set(0,IntValue(1))
      else
        is.set(0,IntValue(2))
      is.set(1,IntValue(9-i))
      is.set(2,IntValue(3))
      is.set(3,IntValue(100))
      is.insertRow
      i+=1
    }
    assert(m.insert("testdb","testtable",is)==20)
  }

  test("Full primary key select") {
    val query = Query("testdb","testtable",
                      Seq("a","b","c"),
                      Seq(EqualityPredicate("a",1),
                          EqualityPredicate("b",2)))
    val rs = m.query(query)
    assert(rs.size == 1)
    assert(rs.next == true)
    assert(rs.getInt(0) == 1)
    assert(rs.getInt(1) == 2)
    assert(rs.getInt(2) == 3)
  }

  test("Partial primary key select") {
    val query = Query("testdb","testtable",
                      Seq("a","b","c"),
                      Seq(EqualityPredicate("a",1)))
    val rs = m.query(query)
    assert(rs.size == 10)
    var i = 0
    while (rs.next) {
      assert(rs.getInt(0) == 1)
      assert(rs.getInt(1) == i)
      assert(rs.getInt(2) == 3)
      i+=1
    }
    assert(i==10)
  }

  test("Full iteration") {
    val query = Query("testdb","testtable",
                      Seq("a","b","c"),Seq())
    val rs = m.query(query)

    assert(rs.size == 20)

    var i = 0
    while (rs.next) {
      if (i < 10) {
        assert(rs.getInt(0) == 1)
        assert(rs.getInt(1) == i)
      }
      else {
        assert(rs.getInt(0) == 2)
        assert(rs.getInt(1) == (i-20))
      }

      assert(rs.getInt(2) == 3)
      i+=1
    }
  }

}
