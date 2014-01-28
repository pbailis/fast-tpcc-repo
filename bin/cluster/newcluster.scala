#!/bin/sh
exec scala -cp ../../target/scala-2.10/velox-assembly-0.1.jar -savecompiled "$0" "$@"
!#


import awscala.Region
import awscala.ec2.InstanceType
import java.io.File
import edu.berkeley.velox.ec2._

// todo: use scopt or something like that to get args for name etc
val clusterName = "exampleCluster"
val instances = 1
val securityGroups = Seq("velox")
val pemFile = new File("/home/nick/research/.ec2/nick-oregon.pem")

val cluster = new Cluster(clusterName,instances,"ami-8885e5b8",InstanceType.Cr1_8xlarge,securityGroups,pemFile)
cluster.start

