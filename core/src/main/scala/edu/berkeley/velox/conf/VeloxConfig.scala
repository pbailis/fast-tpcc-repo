package edu.berkeley.velox.conf

import edu.berkeley.velox.net.{NetworkService,ArrayNetworkService,NIONetworkService}
import java.net.InetSocketAddress
import edu.berkeley.velox.NetworkDestinationHandle

object VeloxConfig {
  var internalServerAddresses: Map[NetworkDestinationHandle, InetSocketAddress] = null
  var frontendServerAddresses: Map[NetworkDestinationHandle, InetSocketAddress] = null

  var internalServerPort = -1
  var externalServerPort = -1
  var partitionId: NetworkDestinationHandle = -1
  var bootstrapConnectionWaitSeconds = 2
  var tcpNoDelay: Boolean = true
  var numBuffersPerRing = 2
  var partitionList: Array[NetworkDestinationHandle] = null

  var networkService = "array"

  def initialize(cmdLine: Array[String]): Boolean = {
    val ret = parser.parse(cmdLine)
    if(internalServerAddresses != null) {
      partitionList = internalServerAddresses.keys.toArray
    }
    ret
  }

  val parser = new scopt.OptionParser[Unit]("velox") {
    opt[Int]('i', "id") foreach { i => partitionId = i } text("Partition ID for this server")
    opt[Int]('p', "internal_port") foreach { p => internalServerPort = p } text("Port to listen for internal connections")
    opt[Int]('f', "frontend_port") foreach { p => externalServerPort = p } text("Port to listen for frontend connections")
    opt[Int]("buffers_per_ring") foreach { p => numBuffersPerRing = p } text("Port to listen for frontend connections")
    opt[Int]("bootstrap_time") foreach { p => bootstrapConnectionWaitSeconds = p } text("Time to wait for server connect bootstrap")
    opt[Boolean]("tcp_nodelay") foreach { p => tcpNoDelay = p } text("Enable/disable TCP_NODELAY")
    opt[String]("network_service") foreach { p => networkService = p } text("Which network service to use [array/nio]")

    // 127.0.0.1:8080,127.0.0.1:8081
    opt[String]('c', "internal_cluster") foreach {
      c => internalServerAddresses = c.split(",").zipWithIndex.map {
        case (hostname, id) =>
          val addr = hostname.split(":")
          (id.asInstanceOf[NetworkDestinationHandle], new InetSocketAddress(addr(0), addr(1).toInt))
      }.toMap
    }  text("Comma-separated list of hostname:port pairs for internal servers in cluster")

    // 127.0.0.1:9001,127.0.0.1:9002
      opt[String]('m', "frontend_cluster") foreach {
        c => frontendServerAddresses = c.split(",").zipWithIndex.map {
          case (hostname, id) =>
            val addr = hostname.split(":")
            (id.asInstanceOf[NetworkDestinationHandle], new InetSocketAddress(addr(0), addr(1).toInt))
        }.toMap
      }  text("Comma-separated list of hostname:port pairs for frontend servers in cluster")
    }

  def getNetworkService(performIDHandshake: Boolean = false, tcpNoDelay: Boolean = true, serverID: Integer = -1): NetworkService = {
    println("Getting network service")
    networkService match {
      case "array" => new ArrayNetworkService(performIDHandshake,tcpNoDelay,serverID)
      case "nio" => new NIONetworkService(performIDHandshake,tcpNoDelay,serverID)
      case _ => throw new Exception(s"Invalid network service type $networkService")
    }
  }
}
