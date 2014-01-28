#!/bin/sh
exec scala -cp target/scala-2.10/velox-assembly-0.1.jar -savecompiled "$0" "$@"
!#

import edu.berkeley.velox.management.ec2._

EC2Config.initialize(args)

val clusterName = EC2Config.cluster_name
val pemFile = EC2Config.pemfile
val numServers = EC2Config.numServers
val numClients = EC2Config.numClients

var curCluster: Cluster = null

if(EC2Config.launch) {
  val cluster = new Cluster(clusterName, numServers, numClients, pemFile=pemFile)
  curCluster = cluster.start
} else {
  curCluster = Cluster.getRunning(EC2Config.cluster_name, EC2Config.pemfile)
}

if(EC2Config.describe) {
  curCluster.publicDns
}


