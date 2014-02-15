package edu.berkeley.velox.datamodel

case class Schema(val pkey: PrimaryKeyDefinition, val columns: (ColumnLabel , Value)*)

class PrimaryKeyDefinition(val value: Seq[ColumnLabel]) extends AnyVal
