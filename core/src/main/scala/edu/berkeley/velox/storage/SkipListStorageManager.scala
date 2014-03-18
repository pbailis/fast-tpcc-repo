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

  def createDatabase(dbName: DatabaseName) = {
    dbs.putIfAbsent(dbName,java.lang.Boolean.TRUE)
  }

  private def tableId(dbName:DatabaseName, tableName: TableName): Int = {
    dbName.## * 31 + tableName.##
  }

  protected def _createTable(dbName:DatabaseName, tableName: TableName) {
    if (dbs.containsKey(dbName)) {
      tbls.putIfAbsent(tableId(dbName,tableName),new ConcurrentSkipListMap[PrimaryKey,Row])
    } else {
      throw new IllegalStateException(s"Table $tableName creation request, but parent database $dbName not found! $dbs")
    }
  }

  protected def _insert(dbName: DatabaseName,tableName: TableName, insertSet: InsertSet): Int = {
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
  // In addition this method returns only
  // the predicates that cannot be statisfied by a key lookup.
  // (which are applied in _query)  If no such predicates exist
  // null is returned
  private def pkFromQuery(query: Query, schema: Schema): (PrimaryKey, Seq[Predicate]) = {
    if (query.predicates.size == 0) return (null,null)
    val sorted = query.predicates.sortWith(_.columnIndex < _.columnIndex)
    var idx = 0
    if (idx != sorted(idx).columnIndex || !sorted(idx).isInstanceOf[EqualityPredicate]) // can't use any of the key
      (null,sorted)
    else {
      val builder = ArrayBuilder.make[Value]

      while (idx < sorted.size && idx == sorted(idx).columnIndex && sorted(idx).isInstanceOf[EqualityPredicate]) {
        builder += sorted(idx).asInstanceOf[EqualityPredicate].value
        idx+=1
      }

      if (idx < sorted.size) {
        // check if last unmatched predicate can be used
        sorted(idx) match {
          case gtp: GreaterThanPredicate => {
            builder += gtp.value
            // don't increment idx here, since we are strictly
            // greater than, so we'll have to verify that we're
            // not equal to.
          }
          case gtep: GreaterThanEqualPredicate => {
            builder += gtep.value
            idx += 1
          }
          case _ => {}
        }
      }

      (PrimaryKey(builder.result), sorted.drop(idx))
    }
  }

  protected def _query(query: Query): ResultSet = {
    val schema = Catalog.getSchema(query.databaseName,query.tableName)
    val (pk,preds) = pkFromQuery(query,schema)
    val nopreds = preds == null || preds.size == 0

    // this stores whether we have predicates that aren't able to match against the
    // primary key, but still refer to columns in the key (i.e. I'm matching on columns 0 and 2)
    val pkpreds = !nopreds && (preds(0).columnIndex < schema.numPkCols)

    val table =  tbls.get(tableId(query.databaseName,query.tableName))
    val rows = new ArrayBuilder.ofRef[Row]

    if (pk != null) { // can do a form of primary key lookup
      val keyit = table.tailMap(pk,true).keySet.iterator
      var prefixmatch = true
      while (prefixmatch && keyit.hasNext()) {
        val key = keyit.next
        if (key.prefixCompare(pk) == 0) { // relevant portion matches
          val row = table.get(key)
          // could use the sorting to be even more clever with this check
          // and set prefixmatch to false if the predicate is a lt/eq
          if (nopreds ||
              (!pkpreds || key.matches(preds)) && row.matches(preds)) {
            val buf = new ArrayBuffer[Value]
            key.projectInto(query.columnIndexes,buf)
            row.projectInto(query.columnIndexes,buf)
            rows += new Row(buf.toArray)
          }
        }
        else {
          // no keys from here on will match the specified predicates
          prefixmatch = false
        }
      }
    } else {
      // no pk specified, just need to go over everything
      val it = table.entrySet.iterator
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
    }
    new ResultSet(rows.result)
  }
}
