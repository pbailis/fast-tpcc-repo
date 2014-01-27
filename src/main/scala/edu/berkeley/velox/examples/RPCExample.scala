package edu.berkeley.velox.examples

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Success, Failure}
import scala.concurrent.duration._

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.rpc._
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.net.{NIONetworkService, BasicNetworkService}
import java.util.concurrent.atomic.AtomicInteger

/**
 * Demonstrate the usage of the RPC layer
 */
object RPCExample extends Logging {

  def main(args: Array[String]) {
    // Parse command line and setup environment
    VeloxConfig.initialize(args)
    logger.debug(s"Starting node ${VeloxConfig.partitionId} ")

    /// Initialize the message service before starting the network -------------------
    // Create an RPC group and initialize by opening all the sockets
    // This should be a blocking call
    val ms = new KryoMessageService()

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
    ms.registerHandler(new FibonacciHandler)

    // Define a message class
    case class DecMsg(body: Int = 0) extends Request[Int]
    // Define the handler for the fibonacci message
    class DecHandler extends MessageHandler[Int, DecMsg] {
      def receive(src: Int, msg: DecMsg): Int = {
        msg.body + 1
      }
    }
    ms.registerHandler(new DecHandler)

    // STart the network -----------------------------------------------------
    // val ns = new BasicNetworkService
    val ns = new NIONetworkService
    ns.setMessageService(ms)
    ns.start()

    // Run the application ---------------------------------------------------
    // Use future to do a non-blocking call
    println(s"Sending Fib request from ${VeloxConfig.partitionId}")
    val f: Future[Int] = ms.send((VeloxConfig.partitionId+1) % VeloxConfig.partitionList.size, FibonacciRequest(42))

    f onComplete {
      case Success(result) => println("Fibonacci(42) = " + result)
      case Failure(t) => println("oops!")
    }

    // Wait for the response
    Await.ready(f, Duration.Inf)



    // Sleep to allow the handler to register
    Thread.sleep(1000)
    println("Starting to ping-pong 1M messages per node for a total of 2M round messages.")
    val msgs = 1000000
    val counter = new AtomicInteger(msgs)
    val other = (VeloxConfig.partitionId+1) % VeloxConfig.partitionList.size
    val start = System.currentTimeMillis()
    for(m <- 0 until msgs) {
      ms.send(other, DecMsg(0)) onSuccess {
        case _ => {
          val c = counter.decrementAndGet()
          if (c == 0) {
            val elapsedTime = (System.currentTimeMillis() - start) / 1000.0
            println(s"Machine ${VeloxConfig.partitionId} finished messages in $elapsedTime")
            println(s"Throughput: ${4000000.0/ elapsedTime}")
          }
        }
      }
    }




  }

}

