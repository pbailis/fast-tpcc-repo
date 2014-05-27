package edu.berkeley.velox

package object ml {
  type Scalar = Int
  type Example = (Vector, Scalar)

  class Vector {
    var arr: Array[Int] = null

    def this(size: Int) {
      this()
      arr = new Array[Int](size)
    }

    def this(arr: Array[Int]) {
      this()
      this.arr = arr
    }

    def size() = arr.size

    def dot(v: Vector): Int = {
      (for ((a, b) <- arr zip v.arr) yield a * b) sum
    }

    // for now, avoid in-place update
    def scale(a: Int): Vector = {
      val ret = new Vector(arr.size)
      for(i <- 0 until arr.size) {
        ret.arr(i) = arr(i)*a
      }

      ret
    }

    def add(v: Vector): Vector = {
      new Vector(for ((a, b) <- arr zip v.arr) yield a + b)
    }

    def subtract(v: Vector): Vector = {
      new Vector(for ((a, b) <- arr zip v.arr) yield a - b)
    }
  }

}
