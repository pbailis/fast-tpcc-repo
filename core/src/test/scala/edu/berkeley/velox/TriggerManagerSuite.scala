package edu.berkeley.velox

import org.scalatest._
import edu.berkeley.velox.trigger._
import edu.berkeley.velox.datamodel._
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{future, Future, Await}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

object Constants {
  // How long a trigger sleeps for.
  val delayMS = 50
  // How many triggers to create.
  val triggers = 2
  // How many rows to update.
  val rows = 2
  val factor = rows * triggers

  val beforeInsertAdd = 1
  val beforeInsertCheck = beforeInsertAdd * factor

  val afterInsertAdd = beforeInsertCheck + 1
  val afterInsertCheck = afterInsertAdd * factor

}

class SyncTrigger(val atomicInt: AtomicInteger) extends AfterInsertRowTrigger with BeforeInsertRowTrigger {
  override def beforeInsert(ctx: TriggerContext, toInsert: Seq[Row]) {
    toInsert.foreach(r => {
      Thread.sleep(Constants.delayMS)
      atomicInt.addAndGet(Constants.beforeInsertAdd)
    })
  }
  override def afterInsert(ctx: TriggerContext, inserted: Seq[Row]): Future[Any] = {
    val futures = inserted.map(r => {
      future {
        Thread.sleep(Constants.delayMS)
        atomicInt.addAndGet(Constants.afterInsertAdd)
      }
    })
    Future.sequence(futures)
  }
}

class AsyncTrigger(val atomicInt: AtomicInteger) extends AfterInsertAsyncRowTrigger {
  override def afterInsertAsync(ctx: TriggerContext, inserted: Seq[Row]): Future[Any] = {
    val futures = inserted.map(r => {
      future {
        Thread.sleep(Constants.delayMS)
        atomicInt.addAndGet(Constants.afterInsertAdd)
      }
    })
    Future.sequence(futures)
  }
}

class TriggerManagerSuite extends FunSuite with BeforeAndAfterEach {

  override def beforeEach() {
    for (i <- 1 to Constants.triggers) {
      TriggerManager.removeTrigger("db", "table", "trigger" + i)
    }
  }

  def setupInsertTest(useAsync:Boolean=false): (AtomicInteger, InsertSet) = {
    val counter = new AtomicInteger(0)
    val testTrigger = if (useAsync) new AsyncTrigger(counter) else new SyncTrigger(counter)
    for (i <- 1 to Constants.triggers) {
      TriggerManager.addTriggerInstance("db", "table", "trigger" + i, testTrigger)
    }
    val insertSet = new InsertSet
    for (i <- 1 to Constants.rows) {
      insertSet.appendRow(Row.nullRow)
    }
    (counter, insertSet)
  }

  def waitForFutures(futures: Seq[Future[Any]]) {
    val f = Future.sequence(futures)
    Await.result(f, Duration.Inf)
  }

  test("beforeInsert gets triggered") {
    val (counter, insertSet) = setupInsertTest()
    TriggerManager.beforeInsert("db", "table", insertSet)
    assertResult(Constants.beforeInsertCheck) { counter.get() }
  }
  test("afterInsert gets triggered") {
    val (counter, insertSet) = setupInsertTest()
    val syncFutures = TriggerManager.afterInsert("db", "table", insertSet)
    assertResult(false) { syncFutures.isEmpty }
    waitForFutures(syncFutures)
    assertResult(Constants.afterInsertCheck) { counter.get() }
  }
  test("beforeDelete gets triggered") (pending)
  test("afterDelete gets triggered") (pending)
  test("beforeUpdate gets triggered") (pending)
  test("afterUpdate gets triggered") (pending)

  test("beforeInsert is synchronous") {
    val (counter, insertSet) = setupInsertTest()
    val t0 = System.currentTimeMillis
    TriggerManager.beforeInsert("db", "table", insertSet)
    val diff = System.currentTimeMillis - t0
    assert(diff >= Constants.factor * Constants.delayMS)
  }
  test("afterInsert is synchronous") {
    val (counter, insertSet) = setupInsertTest()
    val t0 = System.currentTimeMillis
    val syncFutures = TriggerManager.afterInsert("db", "table", insertSet)
    assertResult(false) { syncFutures.isEmpty }
    waitForFutures(syncFutures)
    val diff = System.currentTimeMillis - t0
    // Should take at least as long as the slowest delay.
    assert(diff >= Constants.delayMS)
  }
  test("beforeDelete is synchronous") (pending)
  test("afterDelete is synchronous") (pending)
  test("beforeUpdate is synchronous") (pending)
  test("afterUpdate is synchronous") (pending)

  test("afterInsertAsync gets triggered, and completes") {
    val (counter, insertSet) = setupInsertTest(true)
    val syncFutures = TriggerManager.afterInsert("db", "table", insertSet)
    assertResult(true) { syncFutures.isEmpty }
    val goal = Constants.afterInsertCheck
    var i = 0
    // Wait double the maximum amount of time to see if all the triggers complete.
    while (i < 2 * Constants.factor && counter.get() != goal) {
      Thread.sleep(Constants.delayMS)
      i += 1
    }
    assertResult(Constants.afterInsertCheck) { counter.get() }
  }
  test("afterDeleteAsync gets triggered, and completes") (pending)
  test("afterUpdateAsync gets triggered, and completes") (pending)

  test("afterInsertAsync is asynchronous") {
    val (counter, insertSet) = setupInsertTest(true)
    val t0 = System.currentTimeMillis
    val syncFutures = TriggerManager.afterInsert("db", "table", insertSet)
    assertResult(true) { syncFutures.isEmpty }
    val diff = System.currentTimeMillis - t0
    // Should be faster than the fastest delay.
    assert(diff < Constants.delayMS)
  }
  test("afterDeleteAsync is asynchronous") (pending)
  test("afterUpdateAsync is asynchronous") (pending)

  test("both afterInsert and afterInsertAsync get triggered") {
    val (counterSync, insertSet1) = setupInsertTest()
    val (counterAsync, insertSet2) = setupInsertTest(true)
    val syncFutures = TriggerManager.afterInsert("db", "table", insertSet1)
    assertResult(false) { syncFutures.isEmpty }
    val goal = Constants.afterInsertCheck
    var i = 0
    // Wait double the maximum amount of time to see if all the triggers complete.
    while (i < 2 * Constants.factor && counterAsync.get() != goal) {
      Thread.sleep(Constants.delayMS)
      i += 1
    }
    assertResult(Constants.afterInsertCheck) { counterAsync.get() }
    waitForFutures(syncFutures)
    assertResult(Constants.afterInsertCheck) { counterSync.get() }
  }
  test("both afterDelete and afterDeleteAsync get triggered") (pending)
  test("both afterUpdate and afterUpdateAsync get triggered") (pending)
}
