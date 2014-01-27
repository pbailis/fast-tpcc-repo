package edu.berkeley.velox.net

import edu.berkeley.velox._
import edu.berkeley.velox.rpc.{MessageService, KryoMessageService}
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import com.codahale.metrics.MetricRegistry

trait NetworkService {
  var messageSentMeter = metrics.meter(MetricRegistry.name(getClass.getName, "messages-sent"))
  var messageReceivedMeter = metrics.meter(MetricRegistry.name(getClass.getName, "messages-received"))
  var bytesWrittenMeter = metrics.meter(MetricRegistry.name(getClass.getName, "bytes-written"))
  var bytesSentMeter = metrics.meter(MetricRegistry.name(getClass.getName, "bytes-sent"))
  var bytesReceivedMeter = metrics.meter(MetricRegistry.name(getClass.getName, "bytes-received"))
  var bytesReadMeter = metrics.meter(MetricRegistry.name(getClass.getName, "bytes-read"))

  var messageService: MessageService = null
  def setMessageService(messageService: MessageService)
  def start()
  def send(dst: PartitionId, buffer: Array[Byte])
}
