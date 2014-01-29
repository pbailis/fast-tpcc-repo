package edu.berkeley.velox.management

import java.io.File
import com.typesafe.scalalogging.slf4j.Logging

object ManagementConfig extends Logging {
  var cluster_name: String = null
  private var pemfile_str: String = null
  var pemfile: File = null
  var numServers: Integer = 0
  var numClients: Integer = 0
  var local: Boolean = false

  // COMMANDS BEGIN
  var launch: Boolean = false
  var describe: Boolean = false
  var client_bench: Boolean = false
  var rebuild: Boolean = false
  var terminate:Boolean = false

  var branch: String = "master"
  var remote: String = "git@github.com:amplab/velox.git"
  var deploy_key: String = null

  val parser = new scopt.OptionParser[Unit]("velox") {
    opt[String]('n', "cluster_name") required() foreach { i => cluster_name = i } text("EC2ManagedCluster ID")
    opt[String]('i', "pemfile") foreach { i => pemfile_str = i } text("pem file path")
    opt[Int]('s', "num_servers") foreach { i => numServers = i } text("Number of server instances in cluster")
    opt[Int]('c', "num_clients") foreach { i => numClients = i } text("Number of client instances in cluster")
    opt[Unit]("local") foreach { i => local = true } text("Launch cluster locally (i.e., non-EC2; default: EC2)")

    opt[Unit]("launch") foreach { i => launch = true } text("Launch new cluster")
    opt[Unit]("describe") foreach { i => describe = true } text("Describe existing cluster")
    opt[Unit]("rebuild") foreach { i => rebuild = true } text("Rebuild cluster")
    opt[Unit]("client_bench") foreach { i => client_bench = true } text("Run client benchmark")
    opt[Unit]("terminate") foreach { i => terminate = true } text("Terminate cluster")
    opt[String]("deploy_key") foreach { i => deploy_key = i } text("Path to deploy key for git remote")



    opt[String]("branch") foreach { i => branch = i } text("Branch to rebuild")
    opt[String]("remote") foreach { i => remote = i } text("Remote to rebuild")

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
