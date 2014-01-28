package edu.berkeley.velox.frontend

import edu.berkeley.velox.rpc.{ClientRPCService, Request}
import scala.concurrent._
import scala.concurrent.duration._
import edu.berkeley.velox._
import edu.berkeley.velox.server._
import edu.berkeley.velox.conf.VeloxConfig


class VeloxConnection {
  val ms = new ClientRPCService
  ms.initialize()
  ms.connect(VeloxConfig.frontendServerAddresses.values.toArray)

  /**
   * Puts a value into the datastore at the given key
   * @param k The key to insert at
   * @param newVal The value to insert
   * @return The old value if the key previously existed, null otherwise.
   */
  def putValue(k: Key, newVal: Value): Value = {
    val f: Future[Value] = ms.sendAny(ClientPutRequest(k, newVal))
    Await.result(f, Duration.Inf)
  }

  /**
   * Similar functionality to putValue but does not return the old value
   * @param k the key to insert at
   * @param v the value to insert
   * @return True if the value was replaced, false otherwise.
   */
  def insertValue(k: Key, v: Value): Boolean = {
    val f: Future[Boolean] = ms.sendAny(ClientInsertRequest(k, v))
    Await.result(f, Duration.Inf)

  }

  /**
   *
   * @param k The key whose value to get
   * @return the value if the key exists, otherwise null
   */
  def getValue(k: Key): Value = {
    val f: Future[Value] = ms.sendAny(ClientGetRequest(k))
    Await.result(f, Duration.Inf)
  }
}
