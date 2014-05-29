package edu.berkeley.velox
import scala.language.implicitConversions

package object ml {
  type Target = Int
  type Example = (DoubleVector, Target)
  implicit def intToScalar(x: Int) = new DoubleScalar(x)
  implicit def doubleToScalar(x: Double) = new DoubleScalar(x)
}
