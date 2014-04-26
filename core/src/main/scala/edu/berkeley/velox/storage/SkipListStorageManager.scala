package edu.berkeley.velox.storage

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.catalog.Catalog
import edu.berkeley.velox.datamodel._
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuilder

class SkipListStorageManager extends StorageManager {

  // just to keep track of the databases we've created
  val dbs = new ConcurrentHashMap[DatabaseName,Boolean]

  // the Int here is a hash of dbname,tablename
  val tbls = new ConcurrentHashMap[Int,ConcurrentSkipListMap[PrimaryKey,Row]]()

  Catalog.registerStorageManager(this,true)

  def createDatabaseLocal(dbName: DatabaseName) = {
    dbs.putIfAbsent(dbName,java.lang.Boolean.TRUE)
  }

  private def tableId(dbName:DatabaseName, tableName: TableName): Int = {
    dbName.## * 31 + tableName.##
  }

  protected def _createTableLocal(dbName:DatabaseName, tableName: TableName) {
    if (dbs.containsKey(dbName)) {
      tbls.putIfAbsent(tableId(dbName,tableName),new ConcurrentSkipListMap[PrimaryKey,Row])
    } else {
      throw new IllegalStateException(s"Table $tableName creation request, but parent database $dbName not found! $dbs")
    }
  }

  protected def _insertLocal(dbName: DatabaseName,tableName: TableName, insertSet: InsertSet): Int = {
    val table =  tbls.get(tableId(dbName,tableName))
    val schema = Catalog.getSchema(dbName,tableName)
    if (insertSet.size == 0) 0
    while (insertSet.next) {
      val (key,value) = insertSet.currentRow.splitIntoKeyVal(schema.numPkCols)
      table.put(key,value)
    }
    insertSet.size
  }

  // Extract as much of a primary key as we can from the query
  // to use in accessing our map.  This simply pulls out all
  // equality predicates that can be strung together to form
  // a prefix of the key.  It also uses a gt/gte predicate
  // as the last piece of the key to match, as this will
  // also index the skip list appropriately.
  //
  // Equality predicates only are returned in a separate key
  // as they can be used as the end key in subMap
  //
  // In addition this method returns only
  // the predicates that cannot be statisfied by a key lookup.
  // (which are applied in _query)  If no such predicates exist
  // null is returned

  // The final element of the return tuple should be passed as the
  // "inclusive" argument to tail/subMap for the skip list
  //                                                      StartPoint   EndPoint     ExtraPreds   inclusive
  private def pkFromQuery(query: Query, schema: Schema): (PrimaryKey, PrimaryKey, Seq[Predicate], Boolean) = {
    if (query.predicates.size == 0) return (null,null,null,false)
    val sorted = query.predicates.sortWith(_.columnIndex < _.columnIndex)
    var idx = 0
    if (idx != sorted(idx).columnIndex) {
      (null,null,sorted,true)
    }
    else {
      val builder = ArrayBuilder.make[Value]
      var inclusive = true
      var eqidx = 0

      while (idx < sorted.size && idx == sorted(idx).columnIndex && sorted(idx).isInstanceOf[EqualityPredicate]) {
        builder += sorted(idx).asInstanceOf[EqualityPredicate].value
        idx+=1
        eqidx+=1
      }

      if (idx < sorted.size) {
        // check if last unmatched predicate can be used
        sorted(idx) match {
          case gtp: GreaterThanPredicate => {
            builder += gtp.value
            idx += 1
            inclusive = false
          }
          case gtep: GreaterThanEqualPredicate => {
            builder += gtep.value
            idx += 1
          }
          case _ => {}
        }
      }

      if (idx == 0) // didn't find any usable predicates
        (null,null,sorted,true)
      else {
        val startarr = builder.result

        val endkey =
          if (idx != eqidx && eqidx != 0)
            PrimaryKey(startarr.take(eqidx))
          else if (idx != eqidx) // no equality preds (eqidx is 0 here)
            null
          else
            PrimaryKey(startarr) // only equality / unusable preds

        (PrimaryKey(startarr), endkey, sorted.drop(idx),inclusive)
      }
    }
  }

  protected def _queryLocal(query: Query): ResultSet = {
    val schema = Catalog.getSchema(query.databaseName,query.tableName)
    val (startkey,endkey,preds,inclusive) = pkFromQuery(query,schema)
    val nopreds = preds == null || preds.size == 0

    // this stores whether we have predicates that aren't able to match against the
    // primary key, but still refer to columns in the key (i.e. I'm matching on columns 0 and 2)
    val pkpreds = !nopreds && (preds(0).columnIndex < schema.numPkCols)

    val table =  tbls.get(tableId(query.databaseName,query.tableName))
    val rows = new ArrayBuilder.ofRef[Row]

    val submap =
      if (startkey != null && endkey != null) // can do a form of primary key lookup and have a stop point
        table.subMap(startkey,inclusive,endkey,true)
      else if (startkey != null) // no stop point
        table.tailMap(startkey,inclusive)
      else // need full scan
        table

    val it = submap.entrySet.iterator
    while (it.hasNext) {
      val entry = it.next
      val key = entry.getKey
      val row = entry.getValue
      if (nopreds ||
          (!pkpreds || key.matches(preds)) && row.matches(preds)) {
        val buf = new ArrayBuffer[Value]
        key.projectInto(query.columnIndexes,buf)
        row.projectInto(query.columnIndexes,buf)
        rows += new Row(buf.toArray)
      }
    }
    new ResultSet(rows.result)
  }

  // debug method to print whole table
  def printTable(dbName: DatabaseName, tableName: TableName) {
    val table = tbls.get(tableId(dbName,tableName))
    val it = table.entrySet.iterator
    while (it.hasNext) {
      val entry = it.next
      val key = entry.getKey
      val row = entry.getValue
      println(s"${key.values.mkString} <-> $row")
    }
  }
}
