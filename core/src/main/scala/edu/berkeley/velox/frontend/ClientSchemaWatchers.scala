package edu.berkeley.velox.frontend

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.CuratorWatcher
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.zookeeper.WatchedEvent
import edu.berkeley.velox.catalog.{ServerCatalog, ClientCatalog}
import org.apache.curator.utils.ZKPaths
import edu.berkeley.velox.server.{ZookeeperConnectionUtils => ZKUtils}
import scala.collection.JavaConverters._


// Client Watchers

/*
  Differences:
    - Schema changes aren't blocked on other clients, so the client watchers don't need to decrement the barrier
    - Clients add to ClientCatalog, not ServerCatalog, because they don't have storage managers
 */
class ClientDBWatcher(client: CuratorFramework) extends CuratorWatcher with Logging {
  override def process(event: WatchedEvent) {
    client.synchronized {
      // Find name of new DB and re-add watcher
      val catalogDBs = client.getChildren.usingWatcher(new ClientDBWatcher(client)).forPath(ZKUtils.CATALOG_ROOT)
        .asScala
        .toSet
      val localDBs = ClientCatalog.listLocalDatabases
      val diff = catalogDBs -- localDBs
      logger.error(s"CLIENT: local: $localDBs, catalog: $catalogDBs, diff: $diff")

      if (diff.size == 1) {
        val newDBName = diff.toList(0)
        ClientCatalog._createDatabaseTrigger(newDBName)
        client.getChildren.usingWatcher(new ClientTableWatcher(newDBName, client))
          .forPath(ZKUtils.makeDBPath(newDBName))
      } else if (diff.size == 0) {
        // we already know about all the databases in the catalog.
        logger.warn(s"Client DB watcher activated but all tables accounted for: $catalogDBs")
      } else {
        throw new IllegalStateException(s"DB schema addition issue. DIFF = ${diff.mkString(",")}")
      }
    }
  }
}

/**
 * Watches for changes to the tables of a specific database
 * @param dbname The database to watch
 */
class ClientTableWatcher(dbname: String,
                         client: CuratorFramework) extends CuratorWatcher with Logging{
  override def process(event: WatchedEvent) {
    logger.error(s"client table watcher called: $dbname")
    client.synchronized {
      // Find name of new DB and re-add watcher
      val catalogTables = client.getChildren.usingWatcher(new ClientTableWatcher(dbname, client))
        .forPath(ZKPaths.makePath(ZKUtils.CATALOG_ROOT, dbname))
        .asScala
        .toSet
      val localTables = ClientCatalog.listLocalTables(dbname)
      val diff = catalogTables -- localTables
      logger.error(s"CREATING TABLE IN CLIENT: local: $localTables, catalog: $catalogTables, diff: $diff")
      if (diff.size == 1) {
        val newTableName = diff.toList(0)
        val schemaBytes = client.getData.forPath(ZKUtils.makeTablePath(dbname, newTableName))
        ServerCatalog._createTableTrigger(dbname, newTableName, ZKUtils.bytesToSchema(schemaBytes))
      } else if (diff.size == 0) {
        // we already know about all the tables in the catalog.
        logger.warn(s"Client Table watcher activated but all tables accounted for: $dbname, $catalogTables")
      } else {
        // TODO how should we handle this error?
        throw new IllegalStateException(s"Table Schema addition issue: DIFF = ${diff.mkString(",")}")
      }
    }
  }
} // end TableWatcher
