package edu.berkeley.velox.frontend

import edu.berkeley.velox.rpc.{ClientRPCService, Request}
import scala.concurrent._
import scala.concurrent.duration._
import edu.berkeley.velox.server._
import java.net.InetSocketAddress
import collection.JavaConversions._
import edu.berkeley.velox.datamodel.{Key, Value}


object VeloxConnection {
  def makeConnection(addresses: java.lang.Iterable[InetSocketAddress]): VeloxConnection = {
    return new VeloxConnection(addresses)
  }
}

class VeloxConnection(serverAddresses: Iterable[InetSocketAddress]) {
  val ms = new ClientRPCService(serverAddresses)
  ms.initialize()
  ms.connect(serverAddresses)

  def createDatabase(db: String): Boolean = {
    val f = ms.sendAny(ClientAddDBRequest(db))
    Await.result(f, Duration.Inf) match {
      case r: Boolean => r
      case _ => false
    }
  }

  def addTable(tbl: String, db: String): Boolean = {
    val f = ms.sendAny(ClientAddTableRequest(tbl, db))
    Await.result(f, Duration.Inf) match {
      case r: Boolean => r
      case _ => false
    }
  }

  /**
   * Puts a value into the datastore at the given key
   * @param k The key to insert at
   * @param newVal The value to insert
   * @return The old value if the key previously existed, null otherwise.
   */
  def putValue(tbl: String, db: String, k: Key, newVal: Value): Value = {
    val f = ms.sendAny(ClientPutRequest(tbl, db, k, newVal))
    Await.result(f, Duration.Inf) match {
      case v: Value => v
      case _ => null
    }
  }

  /** Puts a value into the datastore at the given key
    * returns a future immediatly instead of waiting
    *
    * @param k The key to insert at
    * @param newVal Value to insert
    * @return A Future[Value] which will contain the old value (if
    *         it existed) upon completion
    */
  def putValueFuture(tbl: String, db: String, k: Key, newVal: Value): Future[Value] = {
    ms.sendAny(ClientPutRequest(tbl, db, k, newVal))
  }

  /**
   * Similar functionality to putValue but does not return the old value
   * @param k the key to insert at
   * @param v the value to insert
   * @return True if the value was replaced, false otherwise.
   */
  def insertValue(tbl: String, db: String, k: Key, v: Value): Boolean = {
    val f: Future[Boolean] = ms.sendAny(ClientInsertRequest(tbl, db, k, v))
    Await.result(f, Duration.Inf)

  }

  /**
   *
   * @param k The key whose value to get
   * @return the value if the key exists, otherwise null
   */
  def getValue(tbl: String, db: String, k: Key): Value = {
    val f = ms.sendAny(ClientGetRequest(tbl, db, k))
    Await.result(f, Duration.Inf) match {
      case v: Value => v
      case _ => null
    }
  }

  /** Gets a key.  Returns a Future rather than waiting for completion
   *
   * @param k The key whose value to get
   * @return A Future which upon completion will have the value if the key
   * exists, otherwise null
   */
  def getValueFuture(tbl: String, db: String, k: Key): Future[Value] = {
    ms.sendAny(ClientGetRequest(tbl, db, k))
  }
}
