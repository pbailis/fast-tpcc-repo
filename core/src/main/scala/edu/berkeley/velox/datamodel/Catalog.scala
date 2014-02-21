package edu.berkeley.velox.datamodel

import edu.berkeley.velox.server.ZKClient
import java.util.concurrent.{ConcurrentSkipListSet, ConcurrentHashMap}
import edu.berkeley.velox._
import edu.berkeley.velox.storage.StorageManager
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.slf4j.Logging


/**
 * Created by crankshaw on 2/9/14.
 */
class Catalog(storage: StorageManager) extends Logging {

  var zkClient: Option[ZKClient] = None
  var registered = false
  // map of DBname -> map of tablename, table
  val localSchema = new ConcurrentHashMap[String, ConcurrentSkipListSet[String]]()


  def setZKClient(zk: ZKClient) {
    zkClient = Some(zk)
    registered = true
  }

  def listLocalDatabases: Set[String] = {
    // convert from Java to Scala set under the covers. This sucks.
    localSchema.keySet().asScala.toSet
  }

  /**
   * Create a new database instance
   * @param db The name of the database
   * @return true if the database was created, false if it already existed
   */
  def createDatabase(db: String): Boolean = {
    localSchema.putIfAbsent(db, new ConcurrentSkipListSet[String]()) != null
  }

  def listLocalTables(db: String): Set[String] = {
    localSchema.get(db).asScala.toSet
  }


  def createTable(tbl: String, db: String): Boolean = {
    if (!checkDBExistsLocal(db)) {
      throw new IllegalStateException(s"Trying to add table $tbl to database $db which doesn't exist")
    }
    val tableAdded = localSchema.get(db).add(tbl)
    if (tableAdded) {
      storage.addTable(StorageManager.qualifyTableName(db, tbl))
    }
    tableAdded
  }

  def checkDBExistsLocal(db: String): Boolean = {
    localSchema.containsKey(db)
  }

  /*
   * Potentially blocking call to check zookeeper
   */
  def checkTableExists(db: String, table: String): Boolean = {
    require(registered)
    /* Because there can be only one schema change at time
     * and the only way a schema exists on ZK but not locally is
     * if we are in the middle of a schema change, we know that if the
     * db does not exist locally the table doesn't exist and can return false
     * immediately.
     */
    if (!checkDBExistsLocal(db)) {
      false
    } else if (checkTableExistsLocal(db, table)) {
      true
    } else if (zkClient.get.checkTableExistsZookeeper(db, table)) {
      // add to local schema if table exists in zookeeper but not locally
      storage.addTable(StorageManager.qualifyTableName(db, table))
      true
    } else {
      // no record of table, return false
      false
    }
  }

  def checkTableExistsLocal(db: String, table: String): Boolean = {
    checkDBExistsLocal(db) && localSchema.get(db).contains(table)
  }


}
