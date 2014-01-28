package edu.berkeley.velox.ec2

import awscala.Region
import awscala.ec2.RunInstancesRequest
import awscala.ec2.{Instance,InstanceType, KeyPair}
import com.amazonaws.services.ec2.model.Tag
import fr.janalyse.ssh.{SSH,SSHOptions}
import java.io.File
import java.util.concurrent.TimeoutException
import scala.collection.JavaConverters._
import scala.concurrent.{Await,Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

object ClusterState extends Enumeration {
  type ClusterState = Value
  val init, pending, active, closed = Value
}
import ClusterState._

object Cluster {
  /** Gets a cluster object for a running cluster
    * @param name Name of the cluster when first created
    * @param region Region in which cluster was launched
    * @param pemFile Private key file to log into instances
    */
  def getRunning(name: String, region:Region, pemFile: File): Cluster = {
    val ec2 = VeloxEC2.at(region)
    val tag = new Tag("veloxclustername",name)
    val instances = ec2.instances.filter(inst => {
      val tags = inst.tags
      tags.contains("veloxclustername") && tags("veloxclustername").equals(name)
    })

    val icount = instances.size

    if (icount == 0)
      throw new Exception(s"No instances running in a cluster named ${name}")

    val imageId = instances.head.underlying.getImageId
    val itype = InstanceType.T1_Micro // how to get this without grossness?
    val groups = instances.head.underlying.getSecurityGroups.asScala.map(_.getGroupName)

    val c = new Cluster(name,icount,imageId,itype,groups,pemFile,null)

    c.clusterInstances = Some(instances)
    c.curState = active

    c
  }
}

/**
  * Represents a cluster of hosts
  * @param name The name of the cluster, to be used later in getRunning
  * @param instances How many instances to bring up for this cluster
  * @param imageId Amazon ami to use
  * @param instanceType Type of instance to bring up.  See:
  *   https://github.com/seratch/AWScala/blob/develop/src/main/scala/awscala/ec2/InstanceType.scala
  * @param securityGroups Sequence of groups to add instances too
  * @param pemFile Private key file for logging into instances
  * @param spotPrice If not None, spot instances will be requested at this price
  * @param region AWS region to run in, defaults to Oregon
  * @param keyPair Key pair to run instances with.  Defaults to first keypair for your account
  * @return Cluster cluster object
  */
class Cluster(name: String,
  instances: Int,
  imageId: String,
  instanceType: InstanceType,
  securityGroups: Seq[String],
  pemFile: File,
  spotPrice: Option[String] = None,
  region:Region = Region.Oregon,
  keyPair: KeyPair = null) {

  implicit val ec2 = VeloxEC2.at(region)

  private var curState: ClusterState = init
  private var clusterInstances: Option[Seq[Instance]] = None



  //private val sshLogin = PublicKeyLogin("ubuntu", pemFile.getAbsolutePath)
  //private val verifier = new PromiscuousVerifier

  def instances(): Option[Seq[Instance]] = clusterInstances

  def state(): ClusterState = curState

  def start() {
    if (curState != init) return

    curState = pending
    val kp =
      if (keyPair == null) ec2.keyPairs.head
      else keyPair


    val f = spotPrice match {
      case Some(price) => {
        println(s"Will request spot instances at: $price")
        Future(ec2.runSpotAndAwait(imageId,kp,price,Some("velox"), instanceType, instances))
      }
      case None => {
        val req = new RunInstancesRequest(imageId, instances, instances)
          .withKeyName(kp.getKeyName)
          .withInstanceType(instanceType)
          .withSecurityGroupIds(securityGroups.asJavaCollection)
        Future(ec2.runAndAwait(req))
      }
    }


    print("Waiting for instances to come up: ")
    while(!f.isCompleted) {
      try {
        clusterInstances = Some(Await.result(f,Duration(5, "seconds")))
      } catch {
        case e: TimeoutException => print(".")
      }
    }
    // TODO: Check status of each host

    clusterInstances.map(_.map(ec2.tagInstance(_,"veloxclustername",name)))

    println("\nCluster is up")
    curState = active
  }

  def stop() {
    clusterInstances.map(_.map(_.terminate))
    curState = closed
  }

  def publicDns():Seq[String] = {
    clusterInstances.map(_.map(_.publicDnsName)).getOrElse(Seq[String]())
  }

   def execAll(cmd: String) {
     clusterInstances.map(_.foreach(inst => {
       //todo: Make this parallel
       val opts = SSHOptions(host=inst.publicDnsName,username="ubuntu",
         sshKeyFile=Some(pemFile.getAbsolutePath))
       val res = SSH.once(opts){_.execute(cmd)}
       println(s"${inst.publicDnsName}: $res")
     }))
   }

  def execAt(index: Int, cmd: String) {
    clusterInstances.map(insts => {
      val inst = insts(index)
       val opts = SSHOptions(host=inst.publicDnsName,username="ubuntu",
         sshKeyFile=Some(pemFile.getAbsolutePath))
       val res = SSH.once(opts){_.execute(cmd)}
       println(s"${inst.publicDnsName}: $res")
    })
  }

}

//val c = new Cluster("n1", 1, "ami-6aad335a",awscala.ec2.InstanceType.T1_Micro,new java.io.File("/home/nick/research/.ec2/nick-oregon.pem"))
