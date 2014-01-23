package edu.berkeley.velox.net

import edu.berkeley.velox._
import edu.berkeley.velox.rpc.{MessageService, KryoMessageService}
import java.util.concurrent.atomic.AtomicInteger

trait NetworkService {
  var msgSentCounter = new AtomicInteger(0)
  var msgRecvCounter = new AtomicInteger(0)
  var bytesWrittenCounter = new AtomicInteger(0)
  var bytesSentCounter = new AtomicInteger(0)
  var bytesRecvCounter = new AtomicInteger(0)
  var bytesReadCounter = new AtomicInteger(0)


  var messageService: MessageService = null
  def setMessageService(messageService: MessageService)
  def start()
  def send(dst: PartitionId, buffer: Array[Byte])
}
