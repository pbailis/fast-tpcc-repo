/**
  * Slightly more featurefull EC2 client
  */

package edu.berkeley.velox.ec2

import scala.collection.JavaConverters._
import com.amazonaws.services.{ ec2 => aws }
import awscala.ec2.{Instance, InstanceType, KeyPair,EC2}
import awscala.{Credentials,Region}


object VeloxEC2 {

  def apply(credentials: Credentials = Credentials.defaultEnv): VeloxEC2 = new EC2Client(credentials)
  def apply(accessKeyId: String, secretAccessKey: String): VeloxEC2 = apply(Credentials(accessKeyId, secretAccessKey))

  def at(region: Region): VeloxEC2 = apply().at(region)
}

trait VeloxEC2 extends EC2 {

  // override for object
  override
  def at(region: Region): VeloxEC2 = {
    this.setRegion(region)
    this
  }

  private def cancelSpotRequests(ids: Seq[String]) {
    val cancelRequest = new aws.model.CancelSpotInstanceRequestsRequest(ids.asJava)
    cancelSpotInstanceRequests(cancelRequest);
  }

  def tagInstance(instance: Instance, key: String, value: String) {
    val  createTagsRequest = new aws.model.CreateTagsRequest
    createTagsRequest.withResources(instance.instanceId)
      .withTags(new aws.model.Tag(key,value))
    createTags(createTagsRequest)
  }

  def runSpotAndAwait(
    imageId: String,
    keyPair: KeyPair,
    price: String,
    securityGroup: Option[String] = None,
    instanceType: InstanceType = InstanceType.T1_Micro,
    count: Int = 1): Seq[Instance] = {

    // Create the request
    val req = new aws.model.RequestSpotInstancesRequest()
      .withSpotPrice(price)
      .withInstanceCount(count)

    // Set up specification of launch
    val launchSpec = new aws.model.LaunchSpecification()
      .withImageId(imageId)
      .withInstanceType(instanceType)
      .withKeyName(keyPair.getKeyName)

    securityGroup.map(group => {
      val groups = launchSpec.getSecurityGroups
      groups.add(group)
      launchSpec.setSecurityGroups(groups)
    })


    // TODO: Security Group?
    req.setLaunchSpecification(launchSpec)

    runSpotAndAwait(req)
  }



  def runSpotAndAwait(request: aws.model.RequestSpotInstancesRequest): Seq[Instance] = {
    val requestResult = requestSpotInstances(request);

    // Get each individual request for each instance
    val requests = requestResult.getSpotInstanceRequests.
      asScala.map(_.asInstanceOf[aws.model.SpotInstanceRequest])
    // pull out ids
    val spotIds = requests.map(_.getSpotInstanceRequestId)

    // now we wait and loop until all the requests are not in the open state
    val describeReq = new aws.model.DescribeSpotInstanceRequestsRequest().
      withSpotInstanceRequestIds(spotIds.asJavaCollection)
    var reqs: collection.Seq[aws.model.SpotInstanceRequest] = null
    do {

      try {
        reqs = describeSpotInstanceRequests(describeReq).getSpotInstanceRequests.
          asScala.map(_.asInstanceOf[aws.model.SpotInstanceRequest])
      } catch {
        case e: com.amazonaws.AmazonServiceException => reqs = null
      }

      try {
        Thread.sleep(CHECK_INTERVAL);
      } catch {
        // Do nothing because it woke up early.
        case e: InterruptedException => {}
      }

    } while (
      reqs == null ||
      reqs.foldLeft(false)((isOpen,req) => {
        isOpen || req.getState.equals("open")
      })
    )

    // check if all are "active" or if something went wrong
    if (reqs.foldLeft(true)((isActive,req) => {
      val active = req.getState.equals("active")
      if (!active) {
        println(s"Error: Instance ${req} didn't start up")
        false
      }
      isActive
    })) {
      // okay, all active, get the actual instance objects

      val instanceIds = reqs.map(_.getInstanceId)

      // TODO: taken from awsscala, rather inefficient
      def checkStatus(checkIds: Seq[String]): Seq[Instance] =
        instances.filter(i => checkIds.contains(i.instanceId))
      val pendingState = new aws.model.InstanceState().
        withName(aws.model.InstanceStateName.Pending)

      var requestedInstances = checkStatus(instanceIds)

      while (requestedInstances.exists(_.state.getName == pendingState.getName)) {
        Thread.sleep(CHECK_INTERVAL)
        requestedInstances = checkStatus(instanceIds)
      }
      cancelSpotRequests(spotIds)
      requestedInstances
    } else {
      cancelSpotRequests(spotIds)
      Seq[Instance]()
    }
  }
}

class EC2Client(credentials: Credentials = Credentials.defaultEnv)
  extends aws.AmazonEC2Client(credentials)
  with VeloxEC2
