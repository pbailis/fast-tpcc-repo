package edu.berkeley.velox.datamodel


object ResultSet {
  def EMPTY = new ResultSet(new Array[Row](0))
}

class ResultSet(var rows: Seq[Row]) {
  def combine(other: ResultSet) {
    rows :+ other.rows
  }
}
