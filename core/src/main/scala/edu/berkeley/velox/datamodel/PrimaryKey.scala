package edu.berkeley.velox.datamodel

import java.util.Arrays

case class PrimaryKey(values: Array[Value]) {
  override def hashCode() = {
    if (values == null)
      0
    else {
      var result = 1
      var i = 0
      while (i < values.length) {
        result = 31 * result + (if (values(i) == null) 0 else values(i).hashCode())
        i+=1
      }
      result
    }
  }

  override def equals(other: Any) = other match {
    case that: PrimaryKey => this.values.sameElements(that.values)
    case _ => false
  }
}
