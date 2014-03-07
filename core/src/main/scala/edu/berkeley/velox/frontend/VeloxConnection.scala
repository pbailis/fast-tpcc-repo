package edu.berkeley.velox.frontend

import edu.berkeley.velox.rpc.{ClientRPCService, Request}
import scala.concurrent._
import scala.concurrent.duration._
import edu.berkeley.velox.server._
import java.net.InetSocketAddress
import collection.JavaConversions._
import edu.berkeley.velox.benchmark.operation.{TPCCNewOrderResponse, TPCCNewOrderRequest, TPCCLoadRequest, TPCCLoadResponse}
import edu.berkeley.velox.cluster.TPCCPartitioner
import com.typesafe.scalalogging.slf4j.Logging
import java.util.concurrent.Executors


class VeloxConnection(serverAddresses: Iterable[InetSocketAddress], connection_parallelism: Int, whToServer: java.util.HashMap[Int, Int]) extends Logging {
  val ms = new ClientRPCService(serverAddresses)
  ms.networkService.setExecutor(Executors.newCachedThreadPool())
  ms.initialize()

  for(i <- 1 until connection_parallelism)
    ms.connect(serverAddresses)


  def warehouseToServer(W_ID: Int) = {
    if(whToServer == null)
      ((W_ID-1) % serverAddresses.size) +1
    else {
      whToServer.get(W_ID)
    }
  }

  def loadTPCC(W_ID: Int): Future[TPCCLoadResponse] = {
    val serverNo = warehouseToServer(W_ID)
    ms.send(serverNo, new TPCCLoadRequest(W_ID))
  }

  def newOrder(request: TPCCNewOrderRequest): Future[TPCCNewOrderResponse] = {
    ms.send(warehouseToServer(request.W_ID), request)
  }
}
