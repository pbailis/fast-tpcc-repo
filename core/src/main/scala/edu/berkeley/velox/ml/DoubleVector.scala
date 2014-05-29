package edu.berkeley.velox.ml

import java.util.Random

/**
 * Created by jegonzal on 5/28/14.
 */
class DoubleVector {
  var arr: Array[Double] = null


  override
  def toString: String = { "("+arr.mkString(", ")+")" }
  def this(size: Int) {
    this()
    assert(size > 0)
    arr = new Array[Double](size)
  }

  def this(arr: Array[Double]) {
    this()
    this.arr = arr
  }

  def apply(ind: Int): Double = { arr(ind) }

  def update(ind: Int, v: Double) {
    arr(ind) = v
  }

  def size(): Int = arr.size

  def l2norm(): Double = {
    var norm = 0.0
    var i = 0
    val n = size()
    while (i < n) {
      norm += arr(i) * arr(i)
      i += 1
    }
    math.sqrt(norm)
  }

  def dot(v: DoubleVector): Double = {
    assert(size() == v.size())
    val n = size()
    var sum = 0.0
    var i = 0
    while (i < n) {
      sum += arr(i) * v.arr(i)
      i += 1
    }
    sum
  }

  def *(a: Double): DoubleVector = {
    val ret = new DoubleVector(arr.size)
    val n = size()
    var i = 0
    while (i < n) {
      ret.arr(i) = arr(i) * a
      i += 1
    }
    ret
  }

  def *=(a: Double) {
    val n = size()
    var i = 0
    while (i < n) {
      arr(i) *= a
      i += 1
    }
  }

  def +(a: Double): DoubleVector = {
    val ret = new DoubleVector(arr.size)
    val n = size()
    var i = 0
    while (i < n) {
      ret.arr(i) = arr(i) + a
      i += 1
    }
    ret
  }

  def +=(a: Double) {
    val n = size()
    var i = 0
    while (i < n) {
      arr(i) += a
      i += 1
    }
  }

  def -(a: Double): DoubleVector = {
    val ret = new DoubleVector(arr.size)
    val n = size()
    var i = 0
    while (i < n) {
      ret.arr(i) = arr(i) - a
      i += 1
    }
    ret
  }

  def -=(a: Double) {
    val n = size()
    var i = 0
    while (i < n) {
      arr(i) -= a
      i += 1
    }
  }

  def /(a: Double): DoubleVector = {
    val ret = new DoubleVector(arr.size)
    val n = size()
    var i = 0
    while (i < n) {
      ret.arr(i) = arr(i) / a
      i += 1
    }
    ret
  }

  def /=(a: Double) {
    val n = size()
    var i = 0
    while (i < n) {
      arr(i) /= a
      i += 1
    }
  }

  def *(other: DoubleVector): DoubleVector = {
    assert(size() == other.size())
    val ret = new DoubleVector(arr.size)
    val n = size()
    var i = 0
    while (i < n) {
      ret.arr(i) = arr(i) * other(i)
      i += 1
    }
    ret
  }

  def *=(other: DoubleVector) {
    assert(size() == other.size())
    val n = size()
    var i = 0
    while (i < n) {
      arr(i) *= other(i)
      i += 1
    }
  }

  def +(other: DoubleVector): DoubleVector = {
    assert(size() == other.size())
    val ret = new DoubleVector(arr.size)
    val n = size()
    var i = 0
    while (i < n) {
      ret.arr(i) = arr(i) + other(i)
      i += 1
    }
    ret
  }

  def +=(other: DoubleVector) {
    assert(size() == other.size())
    val n = size()
    var i = 0
    while (i < n) {
      arr(i) += other(i)
      i += 1
    }
  }

  def -(other: DoubleVector): DoubleVector = {
    assert(size() == other.size())
    val ret = new DoubleVector(arr.size)
    val n = size()
    var i = 0
    while (i < n) {
      ret.arr(i) = arr(i) - other(i)
      i += 1
    }
    ret
  }

  def -=(other: DoubleVector) {
    assert(size() == other.size())
    val n = size()
    var i = 0
    while (i < n) {
      arr(i) -= other(i)
      i += 1
    }
  }

  def /(other: DoubleVector): DoubleVector = {
    assert(size() == other.size())
    val ret = new DoubleVector(arr.size)
    val n = size()
    var i = 0
    while (i < n) {
      ret.arr(i) = arr(i) / other(i)
      i += 1
    }
    ret
  }

  def /=(other: DoubleVector) {
    assert(size() == other.size())
    val n = size()
    var i = 0
    while (i < n) {
      arr(i) /= other(i)
      i += 1
    }
  }
}


class DoubleScalar (val x: Double) {
  def *(v: DoubleVector): DoubleVector = v * x
  def +(v: DoubleVector): DoubleVector = v + x
  def /(v: DoubleVector): DoubleVector = {
    val n = v.size()
    val ret = new DoubleVector(n)
    var i = 0
    while (i < n) {
      ret(i) = x / v(i)
      i += 1
    }
    ret
  }
  def -(v: DoubleVector): DoubleVector = {
    val n = v.size()
    val ret = new DoubleVector(n)
    var i = 0
    while (i < n) {
      ret(i) = x - v(i)
      i += 1
    }
    ret
  }
}


object DoubleVector {
  val gen = new Random()
  def gaussian(d: Int): DoubleVector = {
    val x = new DoubleVector(d)
    var i = 0
    while (i < d) {
      x(i) = gen.nextGaussian()
      i += 1
    }
    x
  }
}