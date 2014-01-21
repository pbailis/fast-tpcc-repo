package edu.berkeley.velox.rpc

/**
 * The message type represents the type of message sent to a remote machine
 * which must have a response type
 *
 * @tparam R the type of response to this message
 */
trait Request[+R] {
  type Response = R
}

/**
 * The OneWay message indicates that no response is required.
 */
trait OneWayRequest extends Request[Unit]
