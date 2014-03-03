package edu.berkeley.velox.net

import edu.berkeley.velox._
import com.codahale.metrics.MetricRegistry
import java.net.InetSocketAddress
import edu.berkeley.velox.rpc.MessageService
import java.nio.ByteBuffer
import java.util.concurrent.ExecutorService

trait NetworkService {

  var executor: ExecutorService

  var messageSentMeter = metrics.meter(MetricRegistry.name(getClass.getName, "messages-sent"))
  var messageReceivedMeter = metrics.meter(MetricRegistry.name(getClass.getName, "messages-received"))
  var bytesWrittenMeter = metrics.meter(MetricRegistry.name(getClass.getName, "bytes-written"))
  var bytesSentMeter = metrics.meter(MetricRegistry.name(getClass.getName, "bytes-sent"))
  var bytesReceivedMeter = metrics.meter(MetricRegistry.name(getClass.getName, "bytes-received"))
  var bytesReadMeter = metrics.meter(MetricRegistry.name(getClass.getName, "bytes-read"))

  var messageService: MessageService = null
  def setMessageService(messageService: MessageService)

  def start()

  def configureInboundListener(port: Integer)

  /*
   * Connect to remote address and retain handle.
   */
  def connect(handle: NetworkDestinationHandle, address: InetSocketAddress)
  def connect(address: InetSocketAddress): NetworkDestinationHandle
  def disconnect(which: NetworkDestinationHandle)

  def send(dst: NetworkDestinationHandle, buffer: ByteBuffer)
  def sendAny(buffer: ByteBuffer)

  def setExecutor(executor: ExecutorService = null): ExecutorService


  def blockForConnections(numConnections: Integer)
}
