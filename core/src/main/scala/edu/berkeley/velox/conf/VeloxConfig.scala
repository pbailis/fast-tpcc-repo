package edu.berkeley.velox.conf

import java.net.{InetAddress, InetSocketAddress}
import edu.berkeley.velox.net.{NetworkService,ArrayNetworkService,NIONetworkService}
import edu.berkeley.velox.NetworkDestinationHandle

object VeloxConfig {


  var frontendServerAddresses: Map[NetworkDestinationHandle, InetSocketAddress] = null
  var zookeeperServerAddresses: String = null
  var internalServerPort = -1
  var externalServerPort = -1
  // This is the same hack so that we can block until all internal connections have been established.
  var expectedNumInternalServers = -1
  var bootstrapConnectionWaitSeconds = 2
  var tcpNoDelay: Boolean = true
  var numBuffersPerRing = 32
  var serverIpAddress: String = null.asInstanceOf[String]

  var bufferSize = 16384*8
  var networkService = "array"
  var sweepTime = 500

  def initialize(cmdLine: Array[String]): Boolean = {
    val ret = parser.parse(cmdLine)
    ret
  }

  val parser = new scopt.OptionParser[Unit]("velox") {
    opt[Int]('p', "internal_port") foreach { p => internalServerPort = p } text("Port to listen for internal connections")
    opt[Int]('f', "frontend_port") foreach { p => externalServerPort = p } text("Port to listen for frontend connections")
    opt[Int]("buffers_per_ring") foreach { p => numBuffersPerRing = p } text("Buffers per ring (should be > writing threads)")
    opt[Int]("bootstrap_time") foreach { p => bootstrapConnectionWaitSeconds = p } text("Time to wait for server connect bootstrap")
    opt[Int]('b', "buffer_size") foreach { p => bufferSize = p } text("Size (in bytes) to make the network buffer")
    opt[Int]("sweep_time") foreach { p => sweepTime = p } text("Time the ArrayNetworkService send sweep thread should wait between sweeps")
    opt[Boolean]("tcp_nodelay") foreach { p => tcpNoDelay = p } text("Enable/disable TCP_NODELAY")
    opt[String]("network_service") foreach { p => networkService = p } text("Which network service to use [array/nio]")
    opt[String]("ip_address") foreach { p => serverIpAddress = p } text("IP address of this server. Used for Zookeeper registration")
    opt[Int]("num_servers") foreach { p => expectedNumInternalServers = p } text("Total number of velox servers in the cluster")

    opt[String]('z', "zookeeper_cluster") foreach {
      c => zookeeperServerAddresses = c }  text("Comma-separated list of hostname:port pairs for zookeeper servers in cluster")
  }

  def getNetworkService(name: String, performIDHandshake: Boolean = false, tcpNoDelay: Boolean = true, serverID: Integer = -1): NetworkService = {
    println("Getting network service")
    networkService match {
      case "array" => new ArrayNetworkService(name, performIDHandshake, tcpNoDelay, serverID)
      case "nio" => new NIONetworkService(name, performIDHandshake, tcpNoDelay, serverID)
      case _ => throw new Exception(s"Invalid network service type $networkService")
    }
  }
}
