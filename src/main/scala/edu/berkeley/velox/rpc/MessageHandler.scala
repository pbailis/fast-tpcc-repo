package edu.berkeley.velox.rpc

import scala.reflect.ClassTag
import edu.berkeley.velox.PartitionId

/**
 * The message handler trait handles a particular message type
 *
 * @tparam M The Message type which satisfies the Message trait
 */
trait MessageHandler[+R, -M <: Request[R]] {
  /**
    * The receive function is invoked on message reception on the remote machine
    * and should process the message content and return the appropriate response.
    *
    * @param src the originating machine id
    * @param seqId the sequence number assigned by the originating machine
    * @param msg the actual message body
    * @return the response to be sent to the calling process (future).
    */
  def receive(src: PartitionId, msg: M): R
}

