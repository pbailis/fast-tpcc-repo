package edu.berkeley.velox.examples

import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.frontend.VeloxConnection

/**
 * A simple example client that randomly generates numbers in a given range and
 * counts how many times each number has been generated.
 */
object ExampleClient {

  def main(args: Array[String]) {
    /*
    // Parse client args and setup environment
    println("Starting client")
    VeloxConfig.initialize(args)

    val client = new VeloxConnection(VeloxConfig.frontendServerAddresses.values)
    println("Client initialized")

    val testEntries = List(
      (new Key(0), new Value("dsfhdsfdfs")),
      (new Key(10), new Value("sdfsdfdsfds")),
      (new Key(100), new Value("sdhfds")),
      (new Key(24234), new Value("sdfwufweewrwe")),
      (new Key(78779), new Value("dsfewrhwe")))

    println("Inserting entries")
    testEntries.foreach { e => assert(client.putValue(e._1, e._2) == null) }
    println("All entries inserted.\nRetrieving entries.")
    testEntries.foreach { e => assert(client.getValue(e._1) == e._2) }
    println("Entries retrieved. Ready to shut down.")
    */
    
  } 
}
