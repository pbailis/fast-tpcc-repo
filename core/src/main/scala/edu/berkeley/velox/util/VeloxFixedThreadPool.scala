package edu.berkeley.velox.util

import java.util.concurrent.Executors

object VeloxFixedThreadPool {
  lazy val pool = Executors.newFixedThreadPool(Math.max(8, Runtime.getRuntime.availableProcessors()))
}
