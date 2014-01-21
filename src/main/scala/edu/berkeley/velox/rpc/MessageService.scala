package edu.berkeley.velox.rpc

import java.util.{HashMap => JHashMap}
import scala.reflect.ClassTag
import edu.berkeley.velox.net.NetworkService
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ConcurrentHashMap}
import edu.berkeley.velox.{RequestId, PartitionId}
import scala.concurrent.{Future, Promise}
import edu.berkeley.velox.conf.VeloxConfig
import java.nio.ByteBuffer
import com.twitter.chill.KryoInjection
import util.{Success, Failure}

class MessageService(val router: NetworkService) {
  val nextRequestId = new AtomicInteger()
  val requestMap = new ConcurrentHashMap[RequestId, Promise[Any]]

  /**
   * The collection of handlers for various message types
   */
  val handlers = new JHashMap[Int, MessageHandler[Any, Request[Any]]]()

  val requestExecutor = Executors.newCachedThreadPool

  router.attachMessageService(this)

  /**
   * Register a message handler with the RPC layer.
   *
   * @param h the message handler
   * @tparam R the response type
   * @tparam M the message type
   * @param tag to be able to extract a hashcode for the message type we need its ClassTag
   */
  def registerHandler[R, M <: Request[R]](h: MessageHandler[R, M])(implicit tag: ClassTag[M]) {
    handlers.put(tag.hashCode(), h.asInstanceOf[MessageHandler[Any, Request[Any]]])
  }

  /**
   * Send a message to a remote machine and return the response.  Note that if the response is Unit
   * type then send returns immediately.
   *
   * @param dst
   * @param msg
   * @tparam R
   * @return
   */
  def send[R](dst: PartitionId, msg: Request[R]): Future[R] = {
    // type R = M#Response
    val reqId = nextRequestId.getAndIncrement()
    val p = Promise[R]
    requestMap.put(reqId, p.asInstanceOf[Promise[Any]])

    if (dst == VeloxConfig.partitionId) { // Sending message to self
      sendLocalRequest(reqId, msg)
    } else {
      sendRequestRemote(dst, reqId, msg)
    }

    p.future
  }

  private def sendResponse(dst: PartitionId, requestId: RequestId, response: Any) {
    if (dst == VeloxConfig.partitionId) {
      recvResponse_(requestId, response)
    } else {
      router.send(dst, serializeResponse(requestId, response))
    }
  }

  def sendRequestRemote[R](dst: PartitionId, requestId: RequestId, request: Request[R]) {
    router.send(dst, serializeRequest(requestId, request))
  }

  def serializeRequest[R](requestId: RequestId, msg: Request[R]): Array[Byte] = {
    serializeMessage(requestId, msg, true)
  }

  def serializeResponse(requestId: RequestId, msg: Any): Array[Byte] = {
    serializeMessage(requestId, msg, false)
  }

  /*
  Message bytes are:
  8 bytes: request ID; high-order bit is request or response boolean
  N bytes: serialized message
 */
  def serializeMessage(requestId: RequestId, msg: Any, isRequest: Boolean): Array[Byte] = {
    val firstBB = ByteBuffer.allocate(8)
    var header = requestId & ~(1L << 63)
    if(isRequest) header |= (1L << 63)
    firstBB.putLong(header)
    firstBB.array ++ KryoInjection(msg)
  }

  case class DeserializedMessage(val msg: Any, val requestId: RequestId, val isRequest: Boolean)

  def deserializeMessage(bytes: Array[Byte]): DeserializedMessage = {
    val headerBytes = ByteBuffer.wrap(bytes)
    val headerLong = headerBytes.getLong()
    val isRequest = (headerLong >>> 63) == 1
    val requestId = headerLong & ~(1L << 63)

    // TODO: super expensive!
    KryoInjection.invert(bytes.drop(8)) match {
      case Success(v) => DeserializedMessage(v, requestId, isRequest)
      case Failure(e) => println(e); throw e
    }
  }

  // blocking
  private def recvRequest_(src: PartitionId, requestId: RequestId, msg: Any): Unit = {
    val h = handlers.get(msg.getClass().hashCode())
    val response: Any = h.receive(src, msg.asInstanceOf[Request[Any]])
    sendResponse(src, requestId, response)
  }

  // blocking
  private def recvResponse_(requestId: RequestId, response: Any): Unit = {
    requestMap.remove(requestId) success response
  }

  //create a new task for entire function since we don't want the TCP receiver stalling due to serialization
  def recvRemoteMessage(src: PartitionId, bytes: Array[Byte]) {
    requestExecutor.execute(new Runnable {
      def run() = {
        val DeserializedMessage(msg, requestId, isRequest) = deserializeMessage(bytes)
        if(isRequest) {
          recvRequest_(src, requestId, msg)
        } else {
          recvResponse_(requestId, msg)
        }
      }
    })
  }

  def sendLocalRequest(requestId: RequestId, msg: Any) {
    requestExecutor.execute(new Runnable {
      def run() = {
        recvRequest_(VeloxConfig.partitionId, requestId, msg)
      }
    })
  }
}

// End of class RPC

