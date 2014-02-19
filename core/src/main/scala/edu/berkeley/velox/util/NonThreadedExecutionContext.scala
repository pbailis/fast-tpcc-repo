package edu.berkeley.velox.util

import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

object NonThreadedExecutionContext {
  implicit lazy val context: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(16))
}
