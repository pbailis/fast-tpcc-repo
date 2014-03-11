package edu.berkeley.velox

import edu.berkeley.velox.datamodel._
import org.scalatest.FunSuite
import edu.berkeley.velox.datamodel.DataModelConverters._


class PredicateSuite extends FunSuite {

  test("equality") {
    val iv10 = IntValue(10)
    val iv15 = IntValue(15)
    val svxx = StringValue("xx")
    val svyy = StringValue("yy")

    val p1 = EqualityPredicate("blah",IntValue(10))
    assert(p1(iv10))
    assert(!p1(iv15))

    val p2 = EqualityPredicate("blah",StringValue("xx"))
    assert(p2(svxx))
    assert(!p2(svyy))
  }

  test("less than") {
    val iv9 = IntValue(9)
    val iv15 = IntValue(15)
    val svxx = StringValue("xx")
    val svyy = StringValue("yy")

    val p1 = LessThanPredicate("blah",IntValue(15))
    assert(p1(iv9))
    assert(!p1(iv15))

    val p2 = LessThanEqualPredicate("blah",IntValue(15))
    assert(p2(iv9))
    assert(p2(iv15))

    val p3 = LessThanPredicate("blah",StringValue("yy"))
    assert(p3(svxx))
    assert(!p3(svyy))
  }

  test("greater than") {
    val iv9 = IntValue(9)
    val iv15 = IntValue(15)
    val iv20 = IntValue(20)

    val p1 = GreaterThanPredicate("blah",IntValue(15))
    assert(!p1(iv9))
    assert(!p1(iv15))
    assert(p1(iv20))

    val p2 = GreaterThanEqualPredicate("blah",IntValue(15))
    assert(!p2(iv9))
    assert(p2(iv15))
    assert(p2(iv20))
  }

}
