package edu.berkeley.velox.datamodel

case class PrimaryKey(value: Seq[Value]) {
  override def hashCode() = {
    var ret = 1
    value foreach(
      v => ret *= v.hashCode()
    )
    ret
  }

  override def equals(other: Any) = other match {
    case that: PrimaryKey => this.value.sameElements(that.value)
    case _ => false
  }
}
