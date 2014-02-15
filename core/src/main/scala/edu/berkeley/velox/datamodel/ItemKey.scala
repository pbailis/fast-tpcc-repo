package edu.berkeley.velox.datamodel

abstract class ItemKey {
  override def equals(other: Any): Boolean
  override def hashCode: Int
}

