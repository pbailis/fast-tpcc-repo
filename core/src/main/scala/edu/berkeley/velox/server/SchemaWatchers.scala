package edu.berkeley.velox.server

import org.apache.curator.framework.api.CuratorWatcher
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.WatchedEvent
import edu.berkeley.velox.catalog.Catalog
import edu.berkeley.velox.util.zk.DistributedCountdownLatch
import scala.collection.JavaConverters._
import org.apache.curator.utils.ZKPaths
import edu.berkeley.velox.server.{ZookeeperConnectionUtils => ZKUtils}
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.trigger.TriggerManager

// TODO(crankshaw) this file has _A LOT_ of duplicated code

// Server Watchers



class ServerDBWatcher(client: CuratorFramework,
                      schemaChangeBarrier: DistributedCountdownLatch) extends CuratorWatcher with Logging {

  override def process(event: WatchedEvent) {
    // Find name of new DB and re-add watcher
    val catalogDBs = client.getChildren.usingWatcher(new ServerDBWatcher(client, schemaChangeBarrier)).forPath(ZKUtils.CATALOG_ROOT)
      .asScala
      .toSet
    val localDBs = Catalog.listLocalDatabases
    val diff = catalogDBs -- localDBs
    logger.error(s"SERVER: local: $localDBs, catalog: $catalogDBs, diff: $diff")
    if (diff.size == 1) {
      val newDBName = diff.toList(0)
      Catalog._createDatabaseTrigger(newDBName)
      client.getChildren.usingWatcher(new ServerTableWatcher(newDBName, client, schemaChangeBarrier))
        .forPath(ZKUtils.makeDBPath(newDBName))
    } else if (diff.size == 0) {
      // we already know about all the databases in the catalog.
      logger.warn(s"Server DB watcher activated but all dbs accounted for: $catalogDBs")
    } else {
      throw new IllegalStateException(s"DB schema addition issue. DIFF = ${diff.mkString(",")}")
    }
    // set table watcher on new database
    schemaChangeBarrier.decrement()
  }
}

/**
 * Watches for changes to the tables of a specific database
 * @param dbname The database to watch
 */
class ServerTableWatcher(dbname: String,
                   client: CuratorFramework,
                   schemaChangeBarrier: DistributedCountdownLatch) extends CuratorWatcher with Logging{
  override def process(event: WatchedEvent) {
    // Find name of new DB and re-add watcher
    val catalogTables = client.getChildren.usingWatcher(new ServerTableWatcher(dbname, client, schemaChangeBarrier))
      .forPath(ZKPaths.makePath(ZKUtils.CATALOG_ROOT, dbname))
      .asScala
      .toSet
    val localTables = Catalog.listLocalTables(dbname)
    val diff = catalogTables -- localTables
    if (diff.size == 1) {
      val newTableName = diff.toList(0)
      val schemaBytes = client.getData.forPath(ZKUtils.makeTablePath(dbname, newTableName))
      Catalog._createTableTrigger(dbname, newTableName, ZKUtils.bytesToSchema(schemaBytes))
    } else if (diff.size == 0) {
      // we already know about all the tables in the catalog.
      logger.warn(s"Server Table watcher activated but all tables accounted for: $dbname, $catalogTables")
    } else {
      // TODO how should we handle this error?
      throw new IllegalStateException(s"Table Schema addition issue: DIFF = ${diff.mkString(",")}")
    }
    schemaChangeBarrier.decrement()
  }
} // end TableWatcher


// Watches for new triggers.
// (db, table) watches a specific db.table for new triggers
// (db, null) watches a specific db for new tables
// (null, null) watches for new dbs
class TriggerWatcher(client: CuratorFramework,
                     schemaChangeBarrier: DistributedCountdownLatch,
                     dbName: String = null,
                     tblName: String = null) extends CuratorWatcher {
  override def process(event: WatchedEvent) {
    if (dbName != null && tblName != null) {
      // Find names of new triggers of a single table, and re-add new trigger watcher
      val newTriggers = client.getChildren.usingWatcher(new TriggerWatcher(client, schemaChangeBarrier, dbName, tblName))
      .forPath(ZKUtils.makePath(ZKUtils.TRIGGER_ROOT, dbName, tblName)).asScala.toSet
      processNewTriggers(dbName, tblName, TriggerManager.getNewTriggers(dbName, tblName, newTriggers))
    } else if (dbName != null) {
      // Find names of new tables of a single db, and re-add new table watcher
      val newTables = client.getChildren.usingWatcher(new TriggerWatcher(client, schemaChangeBarrier, dbName, null))
      .forPath(ZKUtils.makePath(ZKUtils.TRIGGER_ROOT, dbName)).asScala.toSet
      processNewTables(dbName, TriggerManager.getNewTables(dbName, newTables))
    } else {
      // Find names of new DBs, and re-add new DB watcher
      val newDBs = client.getChildren.usingWatcher(new TriggerWatcher(client, schemaChangeBarrier, null, null))
      .forPath(ZKUtils.TRIGGER_ROOT).asScala.toSet
      processNewDBs(TriggerManager.getNewDBs(newDBs))
    }
    schemaChangeBarrier.decrement()
  }

  private def processNewDBs(dbs: Set[String]) {
    dbs.foreach(newDB => {
      val newTables = client.getChildren.usingWatcher(new TriggerWatcher(client, schemaChangeBarrier, newDB, null))
      .forPath(ZKUtils.makePath(ZKUtils.TRIGGER_ROOT, newDB))
      .asScala.toSet
      processNewTables(newDB, TriggerManager.getNewTables(newDB, newTables))
    })
  }

  private def processNewTables(db: String, tables: Set[String]) {
    tables.foreach(newTable => {
      val newTriggers = client.getChildren.usingWatcher(new TriggerWatcher(client, schemaChangeBarrier, db, newTable))
      .forPath(ZKUtils.makePath(ZKUtils.TRIGGER_ROOT, db, newTable))
      .asScala.toSet
      processNewTriggers(db, newTable, TriggerManager.getNewTriggers(db, newTable, newTriggers))
    })
  }

  private def processNewTriggers(db: String, table: String, triggers: Set[String]) {
    triggers.foreach(newTrigger => {
      val triggerBytes = client.getData.forPath(ZKUtils.makePath(ZKUtils.TRIGGER_ROOT, db, table, newTrigger))
      TriggerManager._addTrigger(db, table, newTrigger, triggerBytes)
    })
  }
} // end TriggerWatcher
