#!/bin/sh
exec scala -deprecation -cp assembly/target/scala-2.10/velox-assembly-0.1.jar "$0" "$@"
!#

import edu.berkeley.velox.management.ManagementConfig
import edu.berkeley.velox.management.ec2.EC2ManagedCluster
import edu.berkeley.velox.management.local.LocalManagedCluster

import java.lang.InterruptedException
import awscala.ec2.InstanceType


if(!ManagementConfig.initialize(args)) {
  sys.exit(-1)
}

if(ManagementConfig.local) {
  val localCluster = new LocalManagedCluster(ManagementConfig.numServers, ManagementConfig.numClients)

  if(ManagementConfig.client_bench) {
    localCluster.startVeloxServers()
    Thread.sleep(6000)
    localCluster.runVeloxClientScript("velox-client")
  }

  sys.exit(0)
}

val clusterName = ManagementConfig.cluster_name
val pemFile = ManagementConfig.pemfile
val numServers = ManagementConfig.numServers
val numClients = ManagementConfig.numClients

var curCluster: EC2ManagedCluster = null

if(ManagementConfig.launch) {
  val cluster = new EC2ManagedCluster(clusterName, numServers, numClients, pemFile=pemFile)
  curCluster = cluster.start
} else {
  curCluster = EC2ManagedCluster.getRunning(ManagementConfig.cluster_name, numServers, numClients, ManagementConfig.pemfile)
}

if(ManagementConfig.describe) {
  println(curCluster.publicDns)
}

if(ManagementConfig.client_bench) {
  curCluster.startVeloxServers()
  Thread.sleep(6000)
  curCluster.runVeloxClientScript("velox-client")
}

if(ManagementConfig.rebuild) {
  curCluster.rebuildVelox(branch = ManagementConfig.branch,
                          remote = ManagementConfig.remote,
                          deployKey = ManagementConfig.deploy_key)
}

if(ManagementConfig.terminate) {
  curCluster.terminate()
}


