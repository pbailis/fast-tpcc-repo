package edu.berkeley.velox.datamodel

import java.util.Arrays
import scala.collection.mutable.Buffer

case class PrimaryKey(values: Array[Value]) extends Comparable[PrimaryKey] {
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

  /** Test if this key passes all the specified predicates.
    *
    * Only tests predicates that should apply to the columns in the key.
    *
    * @param predicates A sequence of predicates to be tested.
    * @returns true if ALL the specified predicates match, otherwise false
    */
  def matches(predicates: Seq[Predicate]): Boolean = {
    var i = 0
    while (i < predicates.size) {
      if ( predicates(i).columnIndex < values.length &&
           !predicates(i)(values(predicates(i).columnIndex)) )
        return false
      i+=1
    }
    true
  }

  def projectInto(ats: Seq[Int], buf: Buffer[Value]) {
    var i = 0
    while (i < ats.length) {
      if (ats(i) < values.length)
        buf += values(ats(i))
      i+=1
    }
  }

  /** Compare to another key.  Keys can be of different lengths.
    *
    * Comparison is as follows:
    *
    * If any of the elements at shared positions in the two keys are
    * not equal, then the key with the element that is less than, is
    * itself less than the other key.  Likewise for greater than.
    *
    * Only shared elements are compared, which turns partial comparisons
    * into a wildcard for unspecified elements for the purposes of querying.
    * This is correct behavior for predicate application, but be aware that
    * this might not be what is desired for other applications.
    */
  override def compareTo(other: PrimaryKey): Int = {
    val lim = Math.min(values.length,other.values.length)
    var i = 0
    while (i < lim) {
      val c = values(i).compareTo(other.values(i))
      if (c != 0) return c
      i+=1
    }
    0 // all shared elements are equal
  }
}
