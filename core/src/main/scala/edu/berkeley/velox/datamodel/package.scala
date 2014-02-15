package edu.berkeley.velox

package object datamodel {
  val INTEGER_TYPE = IntValue(0)
  val STRING_TYPE = StringValue("")

  type DatabaseName = String
  type TableName = String
}
