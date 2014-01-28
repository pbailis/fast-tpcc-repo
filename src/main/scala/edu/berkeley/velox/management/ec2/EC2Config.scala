package edu.berkeley.velox.management.ec2

import java.io.File
import com.typesafe.scalalogging.slf4j.Logging

object EC2Config extends Logging {
  var cluster_name: String = null
  private var pemfile_str: String = null
  var pemfile: File = null
  var numServers: Integer = 0
  var numClients: Integer = 0

  // COMMANDS BEGIN
  var launch: Boolean = false
  var describe: Boolean = false


  val parser = new scopt.OptionParser[Unit]("velox") {
    opt[String]('n', "cluster_name") required() foreach { i => cluster_name = i } text("Cluster ID")
    opt[String]("pemfile") foreach { i => pemfile_str = i } text("pem file path")
    opt[Unit]("launch") foreach { i => launch = true } text("Launch new cluster")
    opt[Unit]("describe") foreach { i => describe = true } text("Describe existing cluster")
    opt[Int]('s', "num_servers") foreach { i => numServers = i } text("Number of server instances in cluster")
    opt[Int]('c', "num_clients") foreach { i => numClients = i } text("Number of client instances in cluster")
  }

def initialize(cmdLine: Array[String]): Boolean = {
    val ret = parser.parse(cmdLine)

    if(pemfile_str == null) {
      pemfile_str = System.getenv("EC2_PRIVATE_KEY")
    }

    if(pemfile_str == null) {
      logger.error("Missing pem file!")
      return false;
    }

    pemfile = new File(pemfile_str)

    ret
  }
}