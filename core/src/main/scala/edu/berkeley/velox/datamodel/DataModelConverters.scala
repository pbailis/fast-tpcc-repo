package edu.berkeley.velox.datamodel

import scala.language.implicitConversions

object DataModelConverters {
  implicit final def toStringValue(value: String) : StringValue = {
    StringValue(value)
  }

  implicit final def toIntValue(value: Int) : IntValue = {
    IntValue(value)
  }

  implicit final def toColumns[T](value: (String, T)) (implicit f: T => Value) : (ColumnLabel, Value) = {
    (ColumnLabel(value._1), f(value._2))
  }

  implicit final def toInsertSetRepeated(values: (ColumnLabel, Value)*): InsertSet = {
    toInsertSetSeq(values)
  }

  implicit final def toInsertSetSeq(values: Seq[(ColumnLabel, Value)]): InsertSet = {
    val ret = new InsertSet
    ret.newRow
    values.foreach { case (c: ColumnLabel, v: Value) => ret.set(c, v) }
    ret.insertRow
    ret
  }

  implicit final def toPrimaryKeyDefinition(value: String*) : PrimaryKeyDefinition = {
    new PrimaryKeyDefinition(value.seq.map(a => ColumnLabel(a)))
  }

  implicit def stringToColumnID(name: String) : ColumnLabel = { ColumnLabel(name) }
}
