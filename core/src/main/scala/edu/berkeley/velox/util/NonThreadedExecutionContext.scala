package edu.berkeley.velox.util

import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

object NonThreadedExecutionContext {
  implicit lazy val executor = Executors.newFixedThreadPool(32)
  implicit lazy val context: ExecutionContext = ExecutionContext.fromExecutor(executor)
}
