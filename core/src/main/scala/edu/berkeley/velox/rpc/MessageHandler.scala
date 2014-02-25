package edu.berkeley.velox.rpc

import edu.berkeley.velox.NetworkDestinationHandle
import scala.concurrent.Future
import com.typesafe.scalalogging.slf4j.Logging

/**
 * The message handler trait handles a particular message type
 *
 * @tparam M The Message type which satisfies the Message trait
 */
trait MessageHandler[+R, -M <: Request[R]] extends Logging {
  /**
    * The receive function is invoked on message reception on the remote machine
    * and should process the message content and return the appropriate response.
    *
    * @param src the originating machine id
    * @param msg the actual message body
    * @return the response to be sent to the calling process (future).
    */
  def receive(src: NetworkDestinationHandle, msg: M): Future[R]
}

