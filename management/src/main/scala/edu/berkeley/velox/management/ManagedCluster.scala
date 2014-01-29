package edu.berkley.velox.management

trait ManagedCluster {
  def startVeloxServers(jvmOpts: String, inBackground: Boolean=true)
  def runVeloxClientScript(scriptName: String, jvmOpts: String)
  def terminate()
}
