package edu.berkeley.velox.rpc

import edu.berkeley.velox.net.NetworkService
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Semaphore, Executors, ConcurrentHashMap}
import edu.berkeley.velox.{RequestId, NetworkDestinationHandle}
import scala.concurrent.{Future, Promise}
import java.nio.ByteBuffer
import util.{Success, Failure}
import java.net.InetSocketAddress
import scala.reflect.ClassTag
import java.util.{HashMap => JHashMap}
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.util.{VeloxKryoRegistrar,KryoThreadLocal}

// this causes our futures to no thread
import edu.berkeley.velox.util.NonThreadedExecutionContext.context

class MessageWrapper(private val encRequestId: Long, val body: Any) {
  def isRequest: Boolean = encRequestId < 0
  def isResponse: Boolean = encRequestId > 0
  def requestId: Long = math.abs(encRequestId)
}

object MessageWrapper {
  def request(requestId: Long, body: Any) = {
    assert(requestId > 0)
    new MessageWrapper(-requestId, body)
  }
  def response(requestId: Long, body: Any) = {
    assert(requestId > 0)
    new MessageWrapper(requestId, body)
  }
}

abstract class MessageService extends Logging {
  val nextRequestId = new AtomicInteger(1) // start at 1 so MessageWrapper logic works
  val requestMap = new ConcurrentHashMap[RequestId, Promise[Any]]
  var networkService: NetworkService = null
  val name: String
  var serviceID : Integer = -1

  /**
   * The collection of handlers for various message types
   */
  val handlers = new JHashMap[Int, MessageHandler[Any, Request[Any]]]()

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

    if(!msg.isInstanceOf[OneWayRequest]) {
      requestMap.put(reqId, p.asInstanceOf[Promise[Any]])
    }

    if (dst == serviceID) { // Sending message to self
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

    if(!msg.isInstanceOf[OneWayRequest]) {
      requestMap.put(reqId, p.asInstanceOf[Promise[Any]])
    }

    networkService.sendAny(serializeMessage(reqId, msg, isRequest=true))
    p.future
  }

  def sendAll[R](msg: Request[R]): Seq[Future[R]] = {
   val ret = networkService.getConnections map { c =>
     val p = Promise[R]
      val reqId = nextRequestId.getAndIncrement()

       if(!msg.isInstanceOf[OneWayRequest]) {
         requestMap.put(reqId, p.asInstanceOf[Promise[Any]])
       }

      networkService.send(c, serializeMessage(reqId, msg, isRequest=true))
      p.future
    }
    ret.toArray :+ send(serviceID, msg)
  }

  def sendAllRemote[R](msg: Request[R]): Seq[Future[R]] = {
    val ret = networkService.getConnections map { c =>
      val p = Promise[R]
      val reqId = nextRequestId.getAndIncrement()

      if(!msg.isInstanceOf[OneWayRequest]) {
        requestMap.put(reqId, p.asInstanceOf[Promise[Any]])
      }

      networkService.send(c, serializeMessage(reqId, msg, isRequest=true))
      p.future
    }
    ret.toSeq
  }

  private def sendResponse(dst: NetworkDestinationHandle, requestId: RequestId, response: Any) {
    if (dst == serviceID) {
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
  def serializeMessage(requestId: RequestId, msg: Any, isRequest: Boolean): ByteBuffer = {
    val buffer = ByteBuffer.allocate(4096)
    var header = requestId & ~(1L << 63)
    if(isRequest) header |= (1L << 63)
    buffer.putLong(header)
    //val kryo = VeloxKryoRegistrar.getKryo()
    val kryo = KryoThreadLocal.kryoTL.get
    val result = kryo.serialize(msg,buffer)
    //VeloxKryoRegistrar.returnKryo(kryo)
    result.flip
    result
  }

  def deserializeMessage(bytes: ByteBuffer): (Any, RequestId, Boolean) = {
    val headerLong = bytes.getLong()
    val isRequest = (headerLong >>> 63) == 1
    val requestId = headerLong & ~(1L << 63)
    // TODO: use Kryo serializer pool instead
    //val kryo = VeloxKryoRegistrar.getKryo()
    val kryo = KryoThreadLocal.kryoTL.get
    val msg = kryo.deserialize(bytes)
    //VeloxKryoRegistrar.returnKryo(kryo)
    (msg, requestId, isRequest)
  }

  // doesn't block, but does set up a handler that will deliver the message when it's ready
  private def recvRequest_(src: NetworkDestinationHandle, requestId: RequestId, msg: Any): Unit = {
    val key = msg.getClass().hashCode()
    assert(handlers.containsKey(key), {println(s"Assert failed: handlers does not contain ${msg.getClass.getName}")})
    val h = handlers.get(key)

    assert(h != null)
    try {
      h.receive(src, msg.asInstanceOf[Request[Any]]) onComplete {
        case Success(response) => {
          if(!msg.isInstanceOf[OneWayRequest]) {
            sendResponse(src, requestId, response)
          }
        }
        case Failure(t) => logger.error(s"Error receiving message $t", t)
      }
    } catch {
      case e: Exception => logger.error(s"Error handling message $msg", e)
    }
  }

  //create a new task for entire function since we don't want the TCP receiver stalling due to serialization
  def receiveRemoteMessage(src: NetworkDestinationHandle, bytes: ByteBuffer) {
    val (msg, requestId, isRequest) = deserializeMessage(bytes)
    if(isRequest) {
      recvRequest_(src, requestId, msg)
    } else {
      // receive the response message
      val p = requestMap.remove(requestId)
      if (p != null) p success msg
      else logger.error(s"Invalid request id $requestId")
    }
  }

  def sendLocalRequest(requestId: RequestId, msg: Any) {
    recvRequest_(serviceID, requestId, msg)
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

  def connect(addresses: Iterable[InetSocketAddress]) = {
    addresses.foreach(address => networkService.connect(address))
  }

  def disconnect(which: NetworkDestinationHandle) {
    networkService.disconnect(which)
  }
}

// End of class RPC

