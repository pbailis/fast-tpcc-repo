package edu.berkeley.velox.management.local

import edu.berkley.velox.management.ManagedCluster

import scala.sys.process.{ProcessLogger, ProcessIO, Process}


class LocalManagedCluster(numServers: Integer, numClients: Integer) extends ManagedCluster {
  val serverLog = "/tmp/server"
  val veloxInternalPortStart = 8080
  val veloxFrontendPortStart = 8080

  val internalClusterConfigStr=(0 until numServers).map { id => s"127.0.0.1:"+(veloxInternalPortStart+id) }.mkString(",")
  val frontendClusterConfigStr=(0 until numServers).map { id => s"127.0.0.1:"+(veloxInternalPortStart+id) }.mkString(",")

  val defaultJVMOpts="-Xmx128m"

  terminate()

  def startVeloxServers(jvmOpts: String=defaultJVMOpts, inBackground: Boolean=true) {
    (0 until numServers).par.foreach{ id =>
      val runCmd = s"JVM_OPTS='$jvmOpts';" +
                             "bin/kill_local_velox.sh;" +
                            s"bin/velox-server -i $id " +
                            s"-c $internalClusterConfigStr " +
                            s"-p ${veloxInternalPortStart+id} " +
                            s"-f ${veloxFrontendPortStart+id} " +
                           (if(inBackground) s"&> $serverLog-$id & disown" else "")
      println(runCmd)
      val p = Process("bash", Seq("-c", runCmd))
      if(inBackground) {
        p.run
      } else {
        val exitCode = p!
      }
    }

  }

  def runVeloxClientScript(scriptName: String, jvmOpts: String=defaultJVMOpts) {
    val p = runBashToEnd(s"export JVM_OPTS='$jvmOpts';" +
                    s"bin/$scriptName " +
                    s"-m $frontendClusterConfigStr;")
    println(p)
  }

  def terminate() {
    val p = runBashToEnd(s"bin/kill_local_velox.sh")
    println(p)
  }

  def runBashToEnd(cmd: String): String = {
    println(cmd)
    val p = Process("bash", Seq("-c", cmd))
    var out = List[String]()

    val exit = p ! ProcessLogger((s) => out ::= s, (s) => out ::= s)

    out.reverse.mkString("\n")
  }

}
