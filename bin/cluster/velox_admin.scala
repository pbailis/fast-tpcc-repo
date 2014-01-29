#!/bin/sh
exec scala -deprecation -cp assembly/target/scala-2.10/velox-assembly-0.1.jar "$0" "$@"
!#

import edu.berkeley.velox.management.ec2.EC2Cluster
import edu.berkeley.velox.management.ec2.EC2Config
import java.lang.InterruptedException


if(!EC2Config.initialize(args)) {
  sys.exit(-1)
}

val clusterName = EC2Config.cluster_name
val pemFile = EC2Config.pemfile
val numServers = EC2Config.numServers
val numClients = EC2Config.numClients

var curCluster: EC2Cluster = null

if(EC2Config.launch) {
  val cluster = new Cluster(clusterName, numServers, numClients, pemFile=pemFile)
  curCluster = cluster.start
} else {
  curCluster = Cluster.getRunning(EC2Config.cluster_name, numServers, numClients, EC2Config.pemfile)
}

if(EC2Config.describe) {
  println(curCluster.publicDns)
}

if(EC2Config.client_bench) {
  curCluster.startVeloxServers()
  Thread.sleep(6000)
  curCluster.runVeloxClientScript("velox-client")
}

if(EC2Config.rebuild) {
  curCluster.rebuildVelox(branch = EC2Config.branch,
                          remote = EC2Config.remote,
                          deployKey = EC2Config.deploy_key)
}

if(EC2Config.terminate) {
  curCluster.terminate()
}


