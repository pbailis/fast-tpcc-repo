package edu.berkeley.velox.management.ec2

import awscala.Region
import awscala.ec2._
import fr.janalyse.ssh.{SSH, SSHOptions, SSHScp}
import java.io.File
import com.amazonaws.services.ec2.model._
import java.util.concurrent.TimeoutException
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import fr.janalyse.ssh.SSHOptions
import awscala.ec2.RunInstancesRequest
import scala.Some
import java.util
import fr.janalyse.ssh.SSHOptions
import awscala.ec2.RunInstancesRequest
import com.amazonaws.services.ec2.model.IpPermission
import awscala.ec2.Instance
import scala.Some
import awscala.ec2.KeyPair
import awscala.ec2.InstanceType
import scala.sys.process.Process
import edu.berkley.velox.management.ManagedCluster

object EC2ClusterState extends Enumeration {
  type ClusterState = Value
  val init, pending, active, closed = Value
}

import EC2ClusterState._

object EC2ManagedCluster {
  val RUNNING_CODE = 16

  /** Gets a cluster object for a running cluster
    * @param name Name of the cluster when first created
    * @param region Region in which cluster was launched
    * @param pemFile Private key file to log into instances
    */
  def getRunning(name: String, numServers: Int, numClients: Int, pemFile: File, region: Region = Region.Oregon): EC2ManagedCluster = {
    val ec2 = VeloxEC2.at(region)
    val tag = new Tag("veloxclustername", name)
    val instances = ec2.instances.filter(inst => {
      val tags = inst.tags
      tags.contains("veloxclustername") && tags("veloxclustername").equals(name) && inst.state.getCode == RUNNING_CODE
    })

    val icount = instances.size

    if (icount == 0)
      throw new Exception(s"No instances running in a cluster named ${name}")

    if(icount < numClients+numServers) {
      throw new Exception(s"Requested $numClients clients and $numServers instances, but only $icount servers found!")
    }

    val imageId = instances.head.underlying.getImageId
    val itype = InstanceType.T1_Micro // how to get this without grossness?

    val c = new EC2ManagedCluster(name, numServers, numClients, pemFile, imageId, itype, null)

    c.clusterInstances = instances
    c.curState = active

    c
  }
}

/**
 * Represents a cluster of hosts
 * @param name The name of the cluster, to be used later in getRunning
 * @param numServers How many servers in this cluster
 * @param numClients How many clients in this cluster
 * @param imageId Amazon ami to use
 * @param instanceType Type of instance to bring up.  See:
 *                     https://github.com/seratch/AWScala/blob/develop/src/main/scala/awscala/edu.berkeley.velox.management/InstanceType.scala
 * @param pemFile Private key file for logging into instances
 * @param spotPrice If not None, spot instances will be requested at this price
 * @param region AWS region to run in, defaults to Oregon
 * @param keyPair Key pair to run instances with.  Defaults to first keypair for your account
 * @return EC2ManagedCluster cluster object
 */
class EC2ManagedCluster(name: String,
              var numServers: Int,
              var numClients: Int,
              pemFile: File,
              imageId: String = "ami-8885e5b8",
              instanceType: InstanceType = InstanceType.Cr1_8xlarge,
              spotPrice: Option[String] = Option("1.5"),
              region: Region = Region.Oregon,
              keyPair: KeyPair = null) extends ManagedCluster {

  implicit val ec2 = VeloxEC2.at(region)

  private var curState: ClusterState = init
  private var clusterInstances: Seq[Instance] = null

  private var numInstances = numServers + numClients

  val defaultJVMOpts = "-XX:+UseParallelGC -Xmx240G"
  private val VELOX_SECURITY_GROUP = "velox"
  private val veloxDir = "/home/ubuntu/velox"
  private val serverLog = "server.log"
  private val veloxInternalPort = 9000
  private val veloxFrontendPort = 8080


  def setNumServers(ns: Integer) = {
    assert(ns < clusterInstances.size)
    numServers = ns
    numClients = clusterInstances.size - numServers
  }

  def setNumClients(nc: Integer) {
    assert(nc < clusterInstances.size)
    numClients = nc
    numServers = clusterInstances.size - numClients
  }

  //private val sshLogin = PublicKeyLogin("ubuntu", pemFile.getAbsolutePath)
  //private val verifier = new PromiscuousVerifier

  def instances(): Seq[Instance] = clusterInstances

  def state(): ClusterState = curState

  def start() : EC2ManagedCluster = {
    if (curState != init) return null

    curState = pending
    val kp =
      if (keyPair == null) ec2.keyPairs.head
      else keyPair

    if(!ec2.describeSecurityGroups.getSecurityGroups.asScala.filter(a => a.getGroupName == VELOX_SECURITY_GROUP).isEmpty) {
      ec2.deleteSecurityGroup(VELOX_SECURITY_GROUP)
    }

    ec2.createSecurityGroup(new CreateSecurityGroupRequest(VELOX_SECURITY_GROUP, "velox launch"))


    var permission = new IpPermission
    permission.setIpProtocol("tcp")
    permission.setFromPort(0)
    permission.setToPort(65535)
    permission.setIpRanges(Seq("0.0.0.0/0") asJavaCollection)
    var permissionList = new util.ArrayList[IpPermission]()
    permissionList.add(permission)
    ec2.authorizeSecurityGroupIngress(new AuthorizeSecurityGroupIngressRequest(VELOX_SECURITY_GROUP, permissionList))

    val f = spotPrice match {
      case Some(price) => {
        println(s"Will request spot instances at: $$$price")
        Future(ec2.runSpotAndAwait(imageId, kp, price, Some(VELOX_SECURITY_GROUP), instanceType, numInstances))
      }
      case None => {
        val req = new RunInstancesRequest(imageId, numInstances, numInstances)
          .withKeyName(kp.getKeyName)
          .withInstanceType(instanceType)
          .withSecurityGroupIds(Seq(VELOX_SECURITY_GROUP) asJavaCollection)
        Future(ec2.runAndAwait(req))
      }
    }

    println(s"Started ${numInstances}.")
    print("Waiting for instances to come up: ")
    while (!f.isCompleted) {
      try {
        clusterInstances = Some(Await.result(f, Duration(5, "seconds"))).get
      } catch {
        case e: TimeoutException => print(".")
      }
    }
    // TODO: Check status of each host

    clusterInstances.map(ec2.tagInstance(_, "veloxclustername", name))

    println("\nCluster is up")
    curState = active
    this
  }

  override def terminate() {
    clusterInstances.map(_.terminate)

    println(s"Shutting down ${clusterInstances.size} instances in cluster $name in $region")

    curState = closed
  }

  def publicDns(): Seq[String] = {
    if(clusterInstances == null) {
      return Seq[String]()
    }
    clusterInstances.map(_.publicDnsName)
  }

  def runCommandInVeloxAll(cmd: String, inBackground: Boolean = false) {
    execAll(s"cd $veloxDir; "+cmd, inBackground)
  }

  def runCommandInVelox(index: Int, cmd: String, inBackground: Boolean = false) {
    execAt(index, s"cd $veloxDir;"+cmd, inBackground)
  }

  def rebuildVelox(branch: String="master",
                   remote: String="git@github.com:amplab/velox.git",
                   deployKey: String=null) {

    if(deployKey != null) {
      println(s"Uploading deploy key $deployKey to remote servers!")
      sendAll(deployKey, s"/home/ubuntu/.ssh/")
      sendAll(deployKey+".pub", s"/home/ubuntu/.ssh/")
      execAll(s"chmod go-r /home/ubuntu/.ssh/*; echo 'IdentityFile /home/ubuntu/.ssh/${deployKey.split("/").last}' >> /home/ubuntu/.ssh/config")
    }

    runCommandInVeloxAll(s"git remote rm vremote; " +
                         s"git remote add vremote $remote;" +
                         s"git checkout master; git branch -D veloxbranch; " +
                         s"git fetch vremote; git checkout -b veloxbranch vremote/$branch; " +
                         s"git reset --hard vremote/$branch; " +
                          "sbt/sbt assembly")
  }

  def startVeloxServers(jvmOpts: String=defaultJVMOpts, inBackground: Boolean=true) {
    val internalClusterConfigStr= clusterInstances.slice(0, numServers).map {
      instance => instance.publicDnsName+":"+veloxInternalPort
    }.mkString(",")

    (0 until numServers).par.foreach {
      id => runCommandInVelox(id,
                              s"touch test.txt; rm $serverLog; export JVM_OPTS='$jvmOpts';" +
                               "bin/kill_local_velox.sh;" +
                              s"bin/velox-server -i $id " +
                              s"-c $internalClusterConfigStr " +
                              s"-p $veloxInternalPort " +
                              s"-f $veloxFrontendPort " +
                              (if(inBackground) s"&> $serverLog & disown" else ""),
                              inBackground)
    }
  }

  def runVeloxClientScript(scriptName: String, jvmOpts: String=defaultJVMOpts) {
    val externalClusterConfigStr= clusterInstances.slice(0, numServers).map {
      instance => instance.publicDnsName+":"+veloxFrontendPort
    }.mkString(",")

    (numServers until numServers+numClients).par.foreach {
      id => runCommandInVelox(id,
                              s"export JVM_OPTS='$jvmOpts';" +
                               "bin/kill_local_velox.sh;" +
                              s"bin/$scriptName " +
                              s"-m $externalClusterConfigStr;")
    }
  }

  def sendAll(local: String, remote: String) {
    clusterInstances.par.map(inst => {
          val opts = SSHOptions(host = inst.publicDnsName, username = "ubuntu", sshKeyFile = Some(pemFile.getAbsolutePath))
          SSH.once(opts) {
            ssh => ssh.scp { scp => scp.send (local, remote) }
          }
          (inst.publicDnsName)
        })
  }

  def execAll(cmd: String, inBackground: Boolean = false) {
    println(s"Running on all: $cmd")

    clusterInstances.par.map(inst => {
      val opts = SSHOptions(host = inst.publicDnsName, username = "ubuntu", sshKeyFile = Some(pemFile.getAbsolutePath))
      System.out.println(cmd)
      val res = SSH.once(opts) {
        ssh =>
        if(inBackground) {
          Future {
            ssh.execute(cmd)
          }
        } else {
          ssh.execute(cmd)
        }
      }
      (inst.publicDnsName, res)
    }).foreach{ case (host, ret) => println(s"${host}: ${ret}" )}
  }

  def getIPByInstanceIndex(index: Int): String = {
    clusterInstances(index).publicDnsName
  }

  def execAt(index: Int, cmd: String, inBackground: Boolean = false) {
    println(s"Running on ${getIPByInstanceIndex(index)}: $cmd")

    val inst = clusterInstances(index)
    val opts = SSHOptions(host = inst.publicDnsName, username = "ubuntu", sshKeyFile = Some(pemFile.getAbsolutePath))
    val res = SSH.once(opts) {
      ssh =>
      if(inBackground) {
        Future {
          val ret = ssh.execute(cmd)
          println(ret)
        }
      } else {
        ssh.execute(cmd)
      }
    }
    println(s"${inst.publicDnsName}: $res")
  }
}

//val c = new EC2ManagedCluster("n1", 1, "ami-6aad335a",awscala.edu.berkeley.velox.management.InstanceType.T1_Micro,new java.io.File("/home/nick/research/.edu.berkeley.velox.management/nick-oregon.pem"))
