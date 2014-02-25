package edu.berkeley.velox.util

import scala.concurrent.ExecutionContext
import com.typesafe.scalalogging.slf4j.Logging

object NonThreadedExecutionContext {
  implicit lazy val context: ExecutionContext = new NonThreadedExecutionContext
}

class NonThreadedExecutionContext extends ExecutionContext with Logging {

  override
  def execute(r: Runnable):Unit = {
    try {
      r.run()
    } catch {
      case e: Exception => {
        logger.error("Caught exception", e)
        reportFailure(e)
      }
    }
  }

  override
  def reportFailure(tx: Throwable) = tx.printStackTrace()
}
