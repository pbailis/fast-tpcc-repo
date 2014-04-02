package edu.berkeley.velox.trigger

import scala.collection.JavaConverters._
import scala.collection.immutable.{HashMap, List}
import scala.concurrent.{Future, future}
import java.util.concurrent.{ConcurrentHashMap, Executors}
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.rpc.MessageService
import edu.berkeley.velox.cluster.Partitioner

object TriggerManager extends Logging {
  // Holds info about a trigger.
  case class TriggerInfo[T](val name: String, val trigger: T, val isUserTrigger: Boolean)

  // Store all the triggers.
  // Each type of trigger is separated, to avoid one level of lookups during execution.
  // TODO: Is multi-level hashmap best way to store triggers?
  val beforeInsertTriggers =
    new ConcurrentHashMap[DatabaseName,
                          ConcurrentHashMap[TableName, List[TriggerInfo[BeforeInsertRowTrigger]]]]()
  val afterInsertTriggers =
    new ConcurrentHashMap[DatabaseName,
                          ConcurrentHashMap[TableName, List[TriggerInfo[AfterInsertRowTrigger]]]]()
  val beforeDeleteTriggers =
    new ConcurrentHashMap[DatabaseName,
                          ConcurrentHashMap[TableName, List[TriggerInfo[BeforeDeleteRowTrigger]]]]()
  val afterDeleteTriggers =
    new ConcurrentHashMap[DatabaseName,
                          ConcurrentHashMap[TableName, List[TriggerInfo[AfterDeleteRowTrigger]]]]()
  val beforeUpdateTriggers =
    new ConcurrentHashMap[DatabaseName,
                          ConcurrentHashMap[TableName, List[TriggerInfo[BeforeUpdateRowTrigger]]]]()
  val afterUpdateTriggers =
    new ConcurrentHashMap[DatabaseName,
                          ConcurrentHashMap[TableName, List[TriggerInfo[AfterUpdateRowTrigger]]]]()
  val afterInsertAsyncTriggers =
    new ConcurrentHashMap[DatabaseName,
                          ConcurrentHashMap[TableName, List[TriggerInfo[AfterInsertAsyncRowTrigger]]]]()
  val afterDeleteAsyncTriggers =
    new ConcurrentHashMap[DatabaseName,
                          ConcurrentHashMap[TableName, List[TriggerInfo[AfterDeleteAsyncRowTrigger]]]]()
  val afterUpdateAsyncTriggers =
    new ConcurrentHashMap[DatabaseName,
                          ConcurrentHashMap[TableName, List[TriggerInfo[AfterUpdateAsyncRowTrigger]]]]()

  val triggerClassLoader = new TriggerClassLoader
  var messageService: MessageService = null
  var partitioner: Partitioner = null

  val allTriggers = List(beforeInsertTriggers,
                         afterInsertTriggers,
                         beforeDeleteTriggers,
                         afterDeleteTriggers,
                         beforeUpdateTriggers,
                         afterUpdateTriggers,
                         afterInsertAsyncTriggers,
                         afterDeleteAsyncTriggers,
                         afterUpdateAsyncTriggers)

  // Similar to allTriggers, but for just user triggers.
  // This is expensive, but this is only used during addition of triggers (schema change).
  def allUserTriggers = {
    allTriggers.map(m => {
      // m is a map[db, map[table, list[tableinfo]]]
      val dbTablePairs = m.entrySet.asScala.map(t => {
        // t is a mapentry[db, map[table, list[tableinfo]]]
        val tableListPairs = t.getValue.entrySet.asScala.map(l => {
          // l is mapentry[table, list[tableinfo]]
          val triggers = l.getValue.filter(_.isUserTrigger).map(_.name)
          // table, list of triggers
          (l.getKey, triggers)
        })
        // db, map[table, list of triggers]
        (t.getKey, tableListPairs.filter(_._2 != Nil).toMap)
      })
      dbTablePairs.filter(_._2.size > 0).toMap
    })
  }

  def initialize(messageService: MessageService, partitioner: Partitioner) {
    this.messageService = messageService
    this.partitioner = partitioner
  }

  // Store a trigger in the given map.
  // If trigger is null, remove the trigger with the triggerName.
  private def _storeTriggerInMap[T](dbName: String,
                                    tableName: String,
                                    triggerName: String,
                                    trigger: Any,
                                    triggerMap: ConcurrentHashMap[DatabaseName,
                                                                  ConcurrentHashMap[TableName, List[TriggerInfo[T]]]],
                                    isUserTrigger: Boolean) {
    triggerMap.putIfAbsent(dbName, new ConcurrentHashMap[TableName, List[TriggerInfo[T]]]())
    val dbTriggers = triggerMap.get(dbName)
    dbTriggers.synchronized {
      // Serialize updating triggers. Should be fine as long as schema changes are rare.
      val existingList = dbTriggers.get(tableName) match {
        case l: List[TriggerInfo[T]] => l
        case null => Nil
      }
      // Filter out the previous version (if it exists) of the same trigger.
      if (trigger != null) {
        // Insert/replace the trigger with the name.
        val newList = TriggerInfo[T](triggerName, trigger.asInstanceOf[T], isUserTrigger) :: existingList.filterNot(_.name == triggerName)
        dbTriggers.put(tableName, newList)
      } else {
        // trigger is null, so remove the trigger with the name.
        val newList = existingList.filterNot(_.name == triggerName)
        dbTriggers.put(tableName, newList)
      }
    }
  }

  def addTriggerInstance(dbName: DatabaseName, tableName: TableName, triggerName: String, trigger: RowTrigger, isUserTrigger: Boolean=false) {
    // initialize the trigger
    trigger.asInstanceOf[RowTrigger].initialize(dbName, tableName)
    var types: List[String] = Nil

    // Store the trigger in the appropriate maps (possibly more than one).
    if (trigger.isInstanceOf[BeforeInsertRowTrigger]) {
      types = "BeforeInsert" :: types
      _storeTriggerInMap(dbName, tableName, triggerName, trigger, beforeInsertTriggers, isUserTrigger)
    }
    if (trigger.isInstanceOf[AfterInsertRowTrigger]) {
      types = "AfterInsert" :: types
      _storeTriggerInMap(dbName, tableName, triggerName, trigger, afterInsertTriggers, isUserTrigger)
    }
    if (trigger.isInstanceOf[BeforeDeleteRowTrigger]) {
      types = "BeforeDelete" :: types
      _storeTriggerInMap(dbName, tableName, triggerName, trigger, beforeDeleteTriggers, isUserTrigger)
    }
    if (trigger.isInstanceOf[AfterDeleteRowTrigger]) {
      types = "AfterDelete" :: types
      _storeTriggerInMap(dbName, tableName, triggerName, trigger, afterDeleteTriggers, isUserTrigger)
    }
    if (trigger.isInstanceOf[BeforeUpdateRowTrigger]) {
      types = "BeforeUpdate" :: types
      _storeTriggerInMap(dbName, tableName, triggerName, trigger, beforeUpdateTriggers, isUserTrigger)
    }
    if (trigger.isInstanceOf[AfterUpdateRowTrigger]) {
      types = "AfterUpdate" :: types
      _storeTriggerInMap(dbName, tableName, triggerName, trigger, afterUpdateTriggers, isUserTrigger)
    }
    // Async triggers.
    if (trigger.isInstanceOf[AfterInsertAsyncRowTrigger]) {
      types = "AfterInsertAsync" :: types
      _storeTriggerInMap(dbName, tableName, triggerName, trigger, afterInsertAsyncTriggers, isUserTrigger)
    }
    if (trigger.isInstanceOf[AfterDeleteAsyncRowTrigger]) {
      types = "AfterDeleteAsync" :: types
      _storeTriggerInMap(dbName, tableName, triggerName, trigger, afterDeleteAsyncTriggers, isUserTrigger)
    }
    if (trigger.isInstanceOf[AfterUpdateAsyncRowTrigger]) {
      types = "AfterUpdateAsync" :: types
      _storeTriggerInMap(dbName, tableName, triggerName, trigger, afterUpdateAsyncTriggers, isUserTrigger)
    }

    logger.info(s"new trigger: $dbName.$tableName: $triggerName(" + types.mkString(", ") + ")")
  }

  def removeTrigger(dbName: DatabaseName, tableName: TableName, triggerName: String) {
    allTriggers.foreach(m => _storeTriggerInMap(dbName, tableName, triggerName, null, m.asInstanceOf[ConcurrentHashMap[DatabaseName, ConcurrentHashMap[TableName, List[TriggerInfo[RowTrigger]]]]], false))
  }

  // add a user trigger to local server (callback for zookeeper)
  def _addTrigger(dbName: DatabaseName, tableName: TableName, triggerName: String, triggerBytes: Array[Byte]) {
    triggerClassLoader.addClassBytes(triggerName, triggerBytes)
    val triggerClass  = triggerClassLoader.loadClass(triggerName)
    val trigger = triggerClass.newInstance
    addTriggerInstance(dbName, tableName, triggerName, trigger.asInstanceOf[RowTrigger], true)
  }

  private def _findTriggers[T](dbName: DatabaseName,
                               tableName: TableName,
                               triggerMap: ConcurrentHashMap[DatabaseName,
                                                             ConcurrentHashMap[TableName, List[TriggerInfo[T]]]]): Seq[T] = {
    val dbTriggers = triggerMap.get(dbName)
    if (dbTriggers == null) return Nil
    val tableTriggers = dbTriggers.get(tableName)
    if (tableTriggers == null) return Nil
    tableTriggers.map(_.trigger)
  }

  def beforeInsert(dbName: DatabaseName, tableName: TableName, insertSet: InsertSet) {
    val syncTriggers = _findTriggers(dbName, tableName, beforeInsertTriggers)
    if (syncTriggers == Nil) return
    val ctx = new TriggerContext(dbName, tableName, messageService, partitioner)

    val rows = insertSet.getRows

    if (syncTriggers != Nil) {
      // Execute sync triggers.
      syncTriggers.foreach(_.beforeInsert(ctx, rows))
    }
  }

  // Returns a seq of futures. Response should be sent after all returned futures have completed.
  def afterInsert(dbName: DatabaseName, tableName: TableName, insertSet: InsertSet): Seq[Future[Any]] = {
    val syncTriggers = _findTriggers(dbName, tableName, afterInsertTriggers)
    val asyncTriggers = _findTriggers(dbName, tableName, afterInsertAsyncTriggers)
    if (syncTriggers == Nil && asyncTriggers == Nil) return Nil
    val ctx = new TriggerContext(dbName, tableName, messageService, partitioner)

    val rows = insertSet.getRows

    if (asyncTriggers != Nil) {
      // Start async triggers. Forget the futures, since a message response does not
      // have to wait for them.
      asyncTriggers.foreach(trigger => {
        trigger.afterInsertAsync(ctx, rows)
      })
    }

    if (syncTriggers != Nil) {
      // Execute sync triggers.  Return the futures for the synchronous response back.
      val futures = syncTriggers.map(trigger => {
        trigger.afterInsert(ctx, rows)
      })
      return futures
    }

    // No sychronous triggers.  Just return Nil.
    return Nil
  }

  // Returns a set of database names in newDBs, without user triggers.
  def getNewDBs(newDBs: Set[DatabaseName]): Set[DatabaseName] = {
    allUserTriggers.map(newDBs -- _.keySet).reduceLeft(_ ++ _)
  }

  // For a particular db, returns a set of table names in newTables, without user triggers.
  def getNewTables(dbName: String, newTables: Set[TableName]): Set[TableName] = {
    val maps = allUserTriggers.map(_.get(dbName))
    val tables = maps.map(m => {
      // each map is converted to a set of table names
      m match {
        case Some(m2) => m2.keySet
        case None => Set[TableName]()
      }
    }).reduce(_ ++ _)
    newTables -- tables
  }

  // For a particular db.table, returns a set of trigger names in newTriggers, without user triggers.
  def getNewTriggers(dbName: String, tableName: String, newTriggers: Set[String]): Set[String] = {
    val maps = allUserTriggers.map(_.get(dbName))
    val triggers = maps.map(m => {
      // m is option (tablename -> list) map
      m match {
        case Some(m2) => {
          m2.get(tableName) match {
            case Some(l) => l.toSet
            case None => Set[String]()
          }
        }
        case None => Set[String]()
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
