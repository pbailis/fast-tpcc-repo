package edu.berkeley.velox.datamodel

import edu.berkeley.velox.catalog.Catalog
import edu.berkeley.velox.frontend.api.Table
import edu.berkeley.velox.datamodel.api.operation.Operation
import scala.concurrent.Future

case class Query(val databaseName: String, val tableName: String, val columns: Seq[ColumnLabel], val predicate: Option[Predicate]) {
  val columnIndexes = columns.map(Catalog.getSchema(databaseName,tableName).indexOf(_))

  // set the column index for the predicate
  predicate.map(p =>
    {
      val idx = Catalog.getSchema(databaseName,tableName).indexOf(p.column)
      if (idx == -1)
        throw new Exception("Predicate specified over nonexistent column")
      p.columnIndex = idx
    })
}
