package edu.berkeley.velox.util

import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

object NonThreadedExecutionContext {
  implicit lazy val context: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(16))
}

class NonThreadedExecutionContext extends ExecutionContext {

  override
  def execute(r: Runnable):Unit = r.run()

  override
  def reportFailure(tx: Throwable) = tx.printStackTrace()
}
