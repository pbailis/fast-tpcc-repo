package edu.berkeley.velox.datamodel

import scala.collection.JavaConverters._

class PrimaryKey extends Row {
  override def hashCode: Int = {
    hashValues
  }

  override def equals(other: Any) = other match {
    case that: Row => this.columns.keys.asScala.sameElements(that.columns.keys.asScala) &&
                      this.columns.values.asScala.sameElements(that.columns.values.asScala)
    case _ => false
  }
}
