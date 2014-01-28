package edu.berkeley.velox.rpc

import edu.berkeley.velox.net.NetworkService
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ConcurrentHashMap}
import edu.berkeley.velox.{RequestId, NetworkDestinationHandle}
import scala.concurrent.{Future, Promise}
import edu.berkeley.velox.conf.VeloxConfig
import java.nio.ByteBuffer
import com.twitter.chill.KryoInjection
import util.{Success, Failure}
import java.net.InetSocketAddress
import scala.reflect.ClassTag
import java.util.{HashMap => JHashMap}
import com.typesafe.scalalogging.slf4j.Logging

abstract class MessageService extends Logging {
  val nextRequestId = new AtomicInteger()
  val requestMap = new ConcurrentHashMap[RequestId, Promise[Any]]
  var networkService: NetworkService = null
  val name: String

  /**
   * The collection of handlers for various message types
   */
  val handlers = new JHashMap[Int, MessageHandler[Any, Request[Any]]]()

  val requestExecutor = Executors.newFixedThreadPool(16)

  def initialize()

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
  def send[R](dst: NetworkDestinationHandle, msg: Request[R]): Future[R] = {
    // type R = M#Response
    val reqId = nextRequestId.getAndIncrement()
    val p = Promise[R]
    requestMap.put(reqId, p.asInstanceOf[Promise[Any]])
    if (dst == VeloxConfig.partitionId) { // Sending message to self
      sendLocalRequest(reqId, msg)
    } else {
      networkService.send(dst, serializeMessage(reqId, msg, isRequest=true))
    }
    p.future
  }

  def sendAny[R](msg: Request[R]): Future[R] = {
    // type R = M#Response
    val reqId = nextRequestId.getAndIncrement()
    val p = Promise[R]
    requestMap.put(reqId, p.asInstanceOf[Promise[Any]])
    networkService.sendAny(serializeMessage(reqId, msg, isRequest=true))
    p.future
  }

  private def sendResponse(dst: NetworkDestinationHandle, requestId: RequestId, response: Any) {
    if (dst == VeloxConfig.partitionId) {
      requestMap.remove(requestId) success response
    } else {
      networkService.send(dst, serializeMessage(requestId, response, isRequest=false))
    }
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

  def deserializeMessage(bytes: Array[Byte]): (Any, RequestId, Boolean) = {
    val headerBytes = ByteBuffer.wrap(bytes)
    val headerLong = headerBytes.getLong()
    val isRequest = (headerLong >>> 63) == 1
    val requestId = headerLong & ~(1L << 63)
    // TODO: use Kryo serializer pool instead
    KryoInjection.invert(bytes.drop(8)) match {
      case Success(msg) => (msg, requestId, isRequest)
      case Failure(e) => println(e); throw e
    }
  }

  // blocking
  private def recvRequest_(src: NetworkDestinationHandle, requestId: RequestId, msg: Any): Unit = {
    val key = msg.getClass().hashCode()
    assert(handlers.containsKey(key))
    val h = handlers.get(key)
    assert(h != null)
    val response: Any = h.receive(src, msg.asInstanceOf[Request[Any]])
    sendResponse(src, requestId, response)
  }

  //create a new task for entire function since we don't want the TCP receiver stalling due to serialization
  def receiveRemoteMessage(src: NetworkDestinationHandle, bytes: Array[Byte]) {
    requestExecutor.execute(new Runnable {
      def run() = {
        val (msg, requestId, isRequest) = deserializeMessage(bytes)
        if(isRequest) {
          recvRequest_(src, requestId, msg)
        } else {
          // receive the response message
          requestMap.remove(requestId) success msg
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

  def configureInboundListener(port: Integer) {
    networkService.configureInboundListener(port)
  }

  /*
   * Connect to remote address and retain handle.
   */
  def connect(handle: NetworkDestinationHandle, address: InetSocketAddress) {
    networkService.connect(handle, address)
  }

  def connect(address: InetSocketAddress): NetworkDestinationHandle = {
    networkService.connect(address)
  }

  def connect(addresses: Array[InetSocketAddress]) = {
    addresses.foreach(address => networkService.connect(address))
  }

  def disconnect(which: NetworkDestinationHandle) {
    networkService.disconnect(which)
  }
}

// End of class RPC

