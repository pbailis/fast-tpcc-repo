package edu.berkeley.velox.trigger

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.HashMap
import java.util.concurrent.ConcurrentHashMap
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.rpc.MessageService
import edu.berkeley.velox.cluster.Partitioner

object TriggerManager extends Logging {
  // Store all the triggers.
  // Each type of trigger is separated, to avoid one level of lookups during execution
  // stores a tuple per trigger: (triggername, trigger)
  // TODO: Is multi-level hashmap best way to store triggers?
  val beforeInsertTriggers = new ConcurrentHashMap[DatabaseName, ConcurrentHashMap[TableName, ArrayBuffer[(String, BeforeInsertRowTrigger)]]]()
  val afterInsertTriggers = new ConcurrentHashMap[DatabaseName, ConcurrentHashMap[TableName, ArrayBuffer[(String, AfterInsertRowTrigger)]]]()
  val beforeDeleteTriggers = new ConcurrentHashMap[DatabaseName, ConcurrentHashMap[TableName, ArrayBuffer[(String, BeforeDeleteRowTrigger)]]]()
  val afterDeleteTriggers = new ConcurrentHashMap[DatabaseName, ConcurrentHashMap[TableName, ArrayBuffer[(String, AfterDeleteRowTrigger)]]]()
  val beforeUpdateTriggers = new ConcurrentHashMap[DatabaseName, ConcurrentHashMap[TableName, ArrayBuffer[(String, BeforeUpdateRowTrigger)]]]()
  val afterUpdateTriggers = new ConcurrentHashMap[DatabaseName, ConcurrentHashMap[TableName, ArrayBuffer[(String, AfterUpdateRowTrigger)]]]()

  val triggerClassLoader = new TriggerClassLoader
  var messageService: MessageService = null
  var partitioner: Partitioner = null

  def initialize(messageService: MessageService, partitioner: Partitioner) {
    this.messageService = messageService
    this.partitioner = partitioner
  }

  // store a trigger in the given map
  private def _storeTrigger[T](dbName: String,
                               tableName: String,
                               triggerName: String,
                               trigger: Any,
                               triggerMap: ConcurrentHashMap[DatabaseName,
                                                             ConcurrentHashMap[TableName, ArrayBuffer[(String, T)]]]) {
    triggerMap.putIfAbsent(dbName, new ConcurrentHashMap[TableName, ArrayBuffer[(String, T)]]())
    val dbTriggers = triggerMap.get(dbName)
    dbTriggers.putIfAbsent(tableName, new ArrayBuffer)
    val tableTriggers = dbTriggers.get(tableName)
    tableTriggers.append((triggerName, trigger.asInstanceOf[T]))
  }

  // add a trigger to local server (callback for zookeeper)
  def _addTrigger(dbName: DatabaseName, tableName: TableName, triggerName: String, triggerBytes: Array[Byte]) {
    triggerClassLoader.addClassBytes(triggerName, triggerBytes)
    val triggerClass  = triggerClassLoader.loadClass(triggerName)
    val trigger = triggerClass.newInstance

    // initialize the trigger
    trigger.asInstanceOf[RowTrigger].initialize(dbName, tableName)

    logger.info(s"new trigger: $dbName.$tableName: $triggerName")

    // store the trigger in the appropriate maps (possibly more than one).
    if (trigger.isInstanceOf[BeforeInsertRowTrigger]) {
      logger.info("    BeforeInsertRowTrigger")
      _storeTrigger(dbName, tableName, triggerName, trigger, beforeInsertTriggers)
    }
    if (trigger.isInstanceOf[AfterInsertRowTrigger]) {
      logger.info("    AfterInsertRowTrigger")
      _storeTrigger(dbName, tableName, triggerName, trigger, afterInsertTriggers)
    }
    if (trigger.isInstanceOf[BeforeDeleteRowTrigger]) {
      logger.info("    BeforeDeleteRowTrigger")
      _storeTrigger(dbName, tableName, triggerName, trigger, beforeDeleteTriggers)
    }
    if (trigger.isInstanceOf[AfterDeleteRowTrigger]) {
      logger.info("    AfterDeleteRowTrigger")
      _storeTrigger(dbName, tableName, triggerName, trigger, afterDeleteTriggers)
    }
    if (trigger.isInstanceOf[BeforeUpdateRowTrigger]) {
      logger.info("    BeforeUpdateRowTrigger")
      _storeTrigger(dbName, tableName, triggerName, trigger, beforeUpdateTriggers)
    }
    if (trigger.isInstanceOf[AfterUpdateRowTrigger]) {
      logger.info("    AfterUpdateRowTrigger")
      _storeTrigger(dbName, tableName, triggerName, trigger, afterUpdateTriggers)
    }
  }

  private def _findTriggers[T](dbName: DatabaseName,
                               tableName: TableName,
                               triggerMap: ConcurrentHashMap[DatabaseName,
                                                             ConcurrentHashMap[TableName, ArrayBuffer[(String, T)]]]): Seq[T] = {
    val dbTriggers = triggerMap.get(dbName)
    if (dbTriggers == null) return Nil
    val tableTriggers = dbTriggers.get(tableName)
    if (tableTriggers == null) return Nil
    tableTriggers.map(_._2)
  }

  def beforeInsert(dbName: DatabaseName, tableName: TableName, insertSet: InsertSet) {
    val tableTriggers = _findTriggers(dbName, tableName, beforeInsertTriggers)
    if (tableTriggers == Nil) return
    val ctx = new TriggerContext(dbName, tableName, messageService, partitioner)
    // Will this need to be a while loop?
    insertSet.getRows.foreach(row => {
      tableTriggers.foreach(_.beforeInsert(ctx, row))
    })
  }

  def afterInsert(dbName: DatabaseName, tableName: TableName, insertSet: InsertSet) {
    val tableTriggers = _findTriggers(dbName, tableName, afterInsertTriggers)
    if (tableTriggers == Nil) return
    val ctx = new TriggerContext(dbName, tableName, messageService, partitioner)
    // Will this need to be a while loop?
    insertSet.getRows.foreach(row => {
      tableTriggers.foreach(_.afterInsert(ctx, row))
    })
  }

  // Returns a set of database names in newDBs, without triggers.
  def getNewDBs(newDBs: Set[DatabaseName]): Set[DatabaseName] = {
    (newDBs -- beforeInsertTriggers.keySet().asScala.toSet) ++
    (newDBs -- afterInsertTriggers.keySet().asScala.toSet) ++
    (newDBs -- beforeDeleteTriggers.keySet().asScala.toSet) ++
    (newDBs -- afterDeleteTriggers.keySet().asScala.toSet) ++
    (newDBs -- beforeUpdateTriggers.keySet().asScala.toSet) ++
    (newDBs -- afterUpdateTriggers.keySet().asScala.toSet)
  }

  // For a particular db, returns a set of table names in newTables, without triggers.
  def getNewTables(dbName: String, newTables: Set[TableName]): Set[TableName] = {
    val maps = List(beforeInsertTriggers.get(dbName),
                    afterInsertTriggers.get(dbName),
                    beforeDeleteTriggers.get(dbName),
                    afterDeleteTriggers.get(dbName),
                    beforeUpdateTriggers.get(dbName),
                    afterUpdateTriggers.get(dbName))

    val tables = maps.map(m => {
      // each map is converted to a set of table names
      if (m != null) {
        m.keySet().asScala.toSet
      } else {
        Set[TableName]()
      }
    }).reduce(_ ++ _)
    newTables -- tables
  }

  // For a particular db.table, returns a set of trigger names in newTriggers, without triggers.
  def getNewTriggers(dbName: String, tableName: String, newTriggers: Set[String]): Set[String] = {
    val maps = List(beforeInsertTriggers.get(dbName),
                    afterInsertTriggers.get(dbName),
                    beforeDeleteTriggers.get(dbName),
                    afterDeleteTriggers.get(dbName),
                    beforeUpdateTriggers.get(dbName),
                    afterUpdateTriggers.get(dbName))

    val triggers = maps.map(m => {
      // m is (tablename -> arraybuffer) map
      if (m != null) {
        val ab = m.get(tableName)
        // ab is arraybuffer of (triggerName, trigger)
        // convert ab to a set of trigger names
        if (ab != null) {
          ab.map(_._1).toSet
        } else {
          Set[String]()
        }
      } else {
        Set[String]()
      }
    }).reduce(_ ++ _)
    newTriggers -- triggers
  }
}

class TriggerClassLoader extends ClassLoader with Logging {
  @volatile var classBytes = new HashMap[String, Array[Byte]]()

  def addClassBytes(name: String, bytes: Array[Byte]) {
    classBytes.synchronized {
      classBytes += ((name, bytes))
    }
  }

  override def findClass(name: String): Class[_] = {
    classBytes.get(name) match {
      case Some(bytes) => defineClass(name, bytes, 0, bytes.length)
      case None => null
    }
  }
}
