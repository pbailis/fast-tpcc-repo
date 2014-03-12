
package edu.berkeley.velox

import com.facebook.LinkBench._
import java.util.Properties
import java.util.{ArrayList => JArrayList}
import edu.berkeley.velox.frontend.VeloxConnection
import edu.berkeley.velox.catalog.Catalog
import java.util.concurrent.atomic.AtomicBoolean

object LinkStoreVelox {
  var isInitialized = new AtomicBoolean
}

class LinkStoreVelox extends GraphStore {
  var counttable: String = null
  var nodetable: String = null
  var linktable: String = null
  var defaultDB: String = null

  def initialize(p: Properties, currentPhase: Phase, threadId: Int) = {
    counttable = ConfigUtil.getPropertyRequired(p, Config.COUNT_TABLE)
    nodetable = p.getPropertyRequired(Config.NODE_TABLE)
    defaultDB = ConfigUtil.getPropertyRequired(p, Config.DBID)
    linktable = ConfigUtil.getPropertyRequired(p, Config.LINK_TABLE)
    val client = new VeloxConnection

    if(!Catalog.checkDBExistsLocal(defaultDB)) {
      LinkStoreVelox.synchronized {
        if(!LinkStoreVelox.isInitialized.get()) {
          LinkStoreVelox.isInitialized.set(true)
          val db = client.createDatabase(defaultDB)
          //db.createTable(counttable)
          //db.createTable(nodetable)
          //db.createTable(linktable)
        }
      }
    }




  }

  def close = {
  }

  def clearErrors(threadID: Int) = {

  }

  def addLink(dbid: String, a: Link, noinverse: Boolean): Boolean = {
    false
  }

  def deleteLink(dbid: String, id1: Long, link_type: Long, id2: Long, noinverse: Boolean, expunge: Boolean): Boolean = {
    false
  }

  def updateLink(dbid: String, a: Link, noinverse: Boolean): Boolean = {
    false
  }

  def getLink(dbid: String, id1: Long, link_type: Long, id2: Long): Link = {
    null
  }

  def getLinkList(dbid: String, id1: Long, link_type: Long): Array[Link] = {
    null
  }

  def getLinkList(dbid: String, id1: Long, link_type: Long, minTimestamp: Long, maxTimestamp: Long, offset: Int, limit: Int): Array[Link] = {
    null
  }

  def countLinks(dbid: String, id1: Long, link_type: Long): Long = {
    -1
  }

  def addNode(dbid: String, node: com.facebook.LinkBench.Node): Long = {
    -1
  }

  def deleteNode(dbid: String, link_type: Int, id: Long): Boolean = {
    false
  }

  def getNode(dbid: String, link_type: Int,id: Long): Node = {
    null
  }

  def resetNodeStore(dbid: String, startID: Long) {

  }

  def updateNode(dbid: String, node: Node): Boolean = {
    false
  }
}
