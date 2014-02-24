package edu.berkeley.velox.util

import com.typesafe.scalalogging.slf4j.Logging
import scala.concurrent.ExecutionContext

object NonThreadedExecutionContext {
  implicit lazy val context: ExecutionContext = new NonThreadedExecutionContext
}

class NonThreadedExecutionContext extends ExecutionContext with Logging {

  override
  def execute(r: Runnable):Unit = try {
    r.run()
  } catch {
    case t: Throwable => {
      logger.error(s"Exception executing future: ${t.getMessage}",t)
      reportFailure(t)
    }
  }

  override
  def reportFailure(tx: Throwable) = tx.printStackTrace()

}
