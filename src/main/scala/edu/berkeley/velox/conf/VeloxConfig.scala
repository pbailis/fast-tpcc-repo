package edu.berkeley.velox.conf

import java.net.InetSocketAddress
import edu.berkeley.velox.PartitionId

object VeloxConfig {
  var serverAddresses: Map[PartitionId, InetSocketAddress] = null
  var serverPort = -1
  var partitionId: PartitionId = -1
  var bootstrapConnectionWaitSeconds = 2
  var tcpNoDelay: Boolean = true
  var partitionList: Array[PartitionId] = null

  def initialize(cmdLine: Array[String]): Boolean = {
    val ret = parser.parse(cmdLine)
    partitionList = serverAddresses.keys.toArray
    ret
  }

  val parser = new scopt.OptionParser[Unit]("velox") {
    opt[Int]('i', "id") required() foreach { i => partitionId = i } text("Partition ID for this server")
    opt[Int]('p', "port") required() foreach { p => serverPort = p } text("Port to listen for new connections")
    opt[Int]("bootstraptime") foreach { p => bootstrapConnectionWaitSeconds = p } text("Time to wait for server connect bootstrap")
    opt[Boolean]("tcpnodelay") foreach { p => tcpNoDelay = p } text("Enable/disable TCP_NODELAY")

    // 127.0.0.1:8080,127.0.0.1:8081
    opt[String]('c', "cluster") required() foreach {
      c => serverAddresses = c.split(",").zipWithIndex.map {
        case (hostname, id) =>
          val addr = hostname.split(":")
          (id.asInstanceOf[PartitionId], new InetSocketAddress(addr(0), addr(1).toInt))
      }.toMap
    }  text("Comma-separated list of hostname:port pairs for servers in cluster")
  }
}
