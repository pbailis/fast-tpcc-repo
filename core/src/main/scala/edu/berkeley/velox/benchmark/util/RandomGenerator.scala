/** *************************************************************************
  * Copyright (C) 2009 by H-Store Project                                 *
  * Brown University                                                      *
  * Massachusetts Institute of Technology                                 *
  * Yale University                                                       *
  * *
  * Permission is hereby granted, free of charge, to any person obtaining *
  * a copy of this software and associated documentation files (the       *
  * "Software"), to deal in the Software without restriction, including   *
  * without limitation the rights to use, copy, modify, merge, publish,   *
  * distribute, sublicense, and/or sell copies of the Software, and to    *
  * permit persons to whom the Software is furnished to do so, subject to *
  * the following conditions:                                             *
  * *
  * The above copyright notice and this permission notice shall be        *
  * included in all copies or substantial portions of the Software.       *
  * *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
  * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
  * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
  * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
  * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
  * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
  * OTHER DEALINGS IN THE SOFTWARE.                                       *
  * **************************************************************************/
package edu.berkeley.velox.benchmark.util

import java.util.HashSet
import java.util.Random
import java.util.Set
import java.nio.ByteBuffer

class RandomGenerator extends Random {
  def getRandomIntSet(cnt: Int, max: Int): Set[Integer] = {
    assert((cnt <= max))
    val ret: Set[Integer] = new HashSet[Integer]
    do {
      ret.add(this.nextInt(max))
    } while (ret.size < cnt)
    return (ret)
  }

  /**
   * Returns a random int value between minimum and maximum (inclusive)
   *
   * @param minimum
   * @param maximum
   * @return a int in the range [minimum, maximum]. Note that this is inclusive.
   */
  def number(minimum: Int, maximum: Int): Integer = {
    //assert(minimum <= maximum, String.format("%d <= %d", minimum, maximum))
    val range_size: Int = maximum - minimum + 1
    var value: Int = this.nextInt(range_size)
    value += minimum
    assert(minimum <= value && value <= maximum)
    return value
  }

  /**
   * Returns a random long value between minimum and maximum (inclusive)
   *
   * @param minimum
   * @param maximum
   * @return
   */
  def number(minimum: Long, maximum: Long): Long = {
    val range_size: Long = (maximum - minimum) + 1
    var bits: Long = 0L
    var `val`: Long = 0L
    do {
      bits = (this.nextLong << 1) >>> 1
      `val` = bits % range_size
    } while (bits - `val` + range_size < 0L)
    `val` += minimum
    return `val`
  }

  /**
   * @param minimum
   * @param maximum
   * @param excluding
   * @return an int in the range [minimum, maximum], excluding excluding.
   */
  def numberExcluding(minimum: Int, maximum: Int, excluding: Int): Int = {
    var num: Int = number(minimum, maximum - 1)
    if (num >= excluding) {
      num += 1
    }
    return num
  }

  /**
   * Returns a random int in a skewed gaussian distribution of the range
   * Note that the range is inclusive
   * A skew factor of 0.0 means that it's a uniform distribution
   * The greater the skew factor the higher the probability the selected random
   * value will be closer to the mean of the range
   *
   * @param minimum    the minimum random number
   * @param maximum    the maximum random number
   * @param skewFactor the factor to skew the stddev of the gaussian distribution
   */
  def numberSkewed(minimum: Int, maximum: Int, skewFactor: Double): Int = {
    if (skewFactor == 0) return (this.number(minimum, maximum))
    assert(minimum <= maximum)
    val range_size: Int = maximum - minimum + 1
    val mean: Int = range_size / 2
    val stddev: Double = range_size - ((range_size / 1.1) * skewFactor)
    var value: Int = -1
    while (value < 0 || value >= range_size) {
      value = Math.round(this.nextGaussian * stddev).asInstanceOf[Int] + mean
    }
    value += minimum
    assert(minimum <= value && value <= maximum)
    return value
  }

  /**
   * @param decimal_places
   * @param minimum
   * @param maximum
   * @return
   */
  def fixedPoint(decimal_places: Int, minimum: Double, maximum: Double): Double = {
    // we don't care about precision
    (scala.util.Random.nextDouble() % maximum)+minimum
    /*
    assert(decimal_places > 0)
    //assert(minimum < maximum, String.format("%f < %f", minimum, maximum))

    var multiplier = 1
    for(i <- 0 to decimal_places) {
      multiplier *= 10
    }

    val int_min: Int = (minimum * multiplier + 0.5).asInstanceOf[Int]
    val int_max: Int = (maximum * multiplier + 0.5).asInstanceOf[Int]
    return this.number(int_min, int_max).asInstanceOf[Double] / multiplier.asInstanceOf[Double]
    */
  }

  def NURand(A: Int, x: Int, y: Int): Int = {
    val C: Int = 0
    return (((this.number(0, A) | this.number(x, y)) + C) % (y - x + 1)) + x
  }

  /**
   * @return a random alphabetic string with length in range [minimum_length, maximum_length].
   */
  def astring(minimum_length: Int, maximum_length: Int): String = {
    return randomString(minimum_length, maximum_length, 'a', 26)
  }

  /**
   * @return a random numeric string with length in range [minimum_length, maximum_length].
   */
  def nstring(minimum_length: Int, maximum_length: Int): String = {
    return randomString(minimum_length, maximum_length, '0', 10)
  }

  /**
   * @param minimum_length
   * @param maximum_length
   * @param base
   * @param numCharacters
   * @return
   */
  private def randomString(minimum_length: Int, maximum_length: Int, base: Char, numCharacters: Int): String = {
    val length: Int = number(minimum_length, maximum_length)

    if(base.isLetter)
      scala.util.Random.alphanumeric.filter(c => c.isLetter).take(length).mkString
    else
      scala.util.Random.alphanumeric.filter(c => c.isDigit).take(length).mkString
  }
}
