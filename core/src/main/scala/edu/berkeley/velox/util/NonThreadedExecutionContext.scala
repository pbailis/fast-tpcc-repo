package edu.berkeley.velox.util

import scala.concurrent.ExecutionContext

object NonThreadedExecutionContext {
  implicit lazy val context: ExecutionContext = new NonThreadedExecutionContext
}

class NonThreadedExecutionContext extends ExecutionContext {

  override
  def execute(r: Runnable):Unit = r.run()

  override
  def reportFailure(tx: Throwable) = tx.printStackTrace()
}
