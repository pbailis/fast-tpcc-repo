package edu.berkeley.velox.datamodel

case class Schema(val pkey: PrimaryKeyDefinition, val columns: (Column , Value)*)

class PrimaryKeyDefinition(val value: Seq[Column]) extends AnyVal

object KeyConversion {
  implicit final def toPrimaryKeyDefinition(value: String*) : PrimaryKeyDefinition = {
    new PrimaryKeyDefinition(value.seq.map(a => Column(a)))
  }
}
