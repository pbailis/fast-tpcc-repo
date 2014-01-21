package edu.berkeley.velox.examples

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Success, Failure}
import scala.concurrent.duration._

import edu.berkeley.velox.rpc._
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.net.NetworkService

/**
 * Demonstrate the usage of the RPC layer
 */
object RPCExample {

  def main(args: Array[String]) {
    // Parse command line and setup environment
    VeloxConfig.initialize(args)
    val ns = new NetworkService
    ns.initialize()

    // Create an RPC group and initialize by opening all the sockets
    // This should be a blocking call
    val ms = new MessageService(ns)

    // Define a message class
    case class FibonacciRequest (val input: Int) extends Request[Int]
    // Define the handler for the fibonacci message
    class FibonacciHandler extends MessageHandler[Int, FibonacciRequest] {
      def fib(x: Int): Int = if (x < 2) x else fib(x - 1) + fib(x - 2)
      def receive(src: Int, msg: FibonacciRequest): Int = {
        println(s"Message from $src received by ${VeloxConfig.partitionId}")
        fib(msg.input)
      }
    }

    // Register the Handler
    ms.registerHandler(new FibonacciHandler)

    // Use future to do a non-blocking call
    val f: Future[Int] = ms.send((VeloxConfig.partitionId+1) % VeloxConfig.partitionList.size, FibonacciRequest(42))

    f onComplete {
      case Success(result) => println("Fibonacci(42) = " + result)
      case Failure(t) => println("oops!")
    }

    // Wait for the response
    Await.ready(f, Duration.Inf)
  }

}

