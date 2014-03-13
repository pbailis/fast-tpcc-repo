package edu.berkeley.velox.datamodel

import edu.berkeley.velox.catalog.Catalog
import edu.berkeley.velox.datamodel.api.operation.Operation
import edu.berkeley.velox.exceptions.QueryException
import edu.berkeley.velox.frontend.api.Table

case class Query(val databaseName: String, val tableName: String, val columns: Seq[ColumnLabel], val predicates: Seq[Predicate]) {

  // we could make these lazy, but then the syntax checking wouldn't
  // happen on the client
  val columnIndexes = getColumnIndexes
  fillPredicateIndexes

  // these helper methods exist to pull out the column indicies of the requested columns
  // and predicates, without having to serialize a schema, but also without having to
  // lookup the schema for every index

  private def getColumnIndexes(): Seq[Int] = {
    val schema = Catalog.getSchema(databaseName,tableName)
    columns.map(col =>
      { val idx = schema.indexOf(col)
        if (idx == -1)
          throw new QueryException(s"Table $databaseName.$tableName does not contain column $col")
        idx
      })
  }

  private def fillPredicateIndexes() {
    val schema = Catalog.getSchema(databaseName,tableName)
    predicates.foreach(p =>
      { val idx = schema.indexOf(p.column)
        if (idx == -1)
          throw new QueryException(s"Predicate specified over nonexistent column: $p.column")
        p.columnIndex = idx
      })
  }
}
