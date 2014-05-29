package edu.berkeley.velox.ml

import java.util.Random


object SyntheticData {
  val gen = new Random()

  def randomModel(dim: Int, scale: Double = 1.0, sparsity: Double = 1.0): DoubleVector = {
    val w = new DoubleVector(dim)
    for(i <- 0 until dim) {
      if (gen.nextDouble() < sparsity) w(i) = gen.nextGaussian() * scale
      else w(i) = 0.0
    }
    w
  }

  def randomData(model: DoubleVector, n: Int, obsNoise: Double = 0.3): Array[Example] = {
    val data = new Array[Example](n)
    var i = 0
    val d = model.size()
    var numPositive = 0
    while (i < n) {
      val x = DoubleVector.gaussian(d)
      val y = if ((model dot x) > 0.0) { numPositive +=1; 1} else -1
      // Flip the observation
      val noisyY = if (gen.nextDouble() < obsNoise) -y else y
      data(i) = (x, noisyY)
      i += 1
    }
    println(s"Proportion of positive examples: ${numPositive.toDouble / n}")
    data
  }



}
