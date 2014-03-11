package edu.berkeley.velox.server

import org.apache.curator.framework.api.CuratorWatcher
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.WatchedEvent
import edu.berkeley.velox.catalog.ServerCatalog
import edu.berkeley.velox.util.zk.DistributedCountdownLatch
import scala.collection.JavaConverters._
import org.apache.curator.utils.ZKPaths
import edu.berkeley.velox.server.{ZookeeperConnectionUtils => ZKUtils}
import com.typesafe.scalalogging.slf4j.Logging

// TODO(crankshaw) this file has _A LOT_ of duplicated code

// Server Watchers



class ServerDBWatcher(client: CuratorFramework,
                      schemaChangeBarrier: DistributedCountdownLatch) extends CuratorWatcher with Logging {

  override def process(event: WatchedEvent) {
    // Find name of new DB and re-add watcher
    val catalogDBs = client.getChildren.usingWatcher(new ServerDBWatcher(client, schemaChangeBarrier)).forPath(ZKUtils.CATALOG_ROOT)
      .asScala
      .toSet
    val localDBs = ServerCatalog.listLocalDatabases
    val diff = catalogDBs -- localDBs
    logger.error(s"SERVER: local: $localDBs, catalog: $catalogDBs, diff: $diff")
    if (diff.size == 1) {
      val newDBName = diff.toList(0)
      ServerCatalog._createDatabaseTrigger(newDBName)
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
    val localTables = ServerCatalog.listLocalTables(dbname)
    val diff = catalogTables -- localTables
    if (diff.size == 1) {
      val newTableName = diff.toList(0)
      val schemaBytes = client.getData.forPath(ZKUtils.makeTablePath(dbname, newTableName))
      ServerCatalog._createTableTrigger(dbname, newTableName, ZKUtils.bytesToSchema(schemaBytes))
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

