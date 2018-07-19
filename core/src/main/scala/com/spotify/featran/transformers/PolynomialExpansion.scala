/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.featran.transformers

import com.spotify.featran.{FeatureBuilder, FeatureRejection, FlatReader, FlatWriter}
import com.twitter.algebird.Aggregator

/**
 * Transform vector features by expanding them into a polynomial space, which is formulated by an
 * n-degree combination of original dimensions.
 *
 * Missing values are transformed to zero vectors.
 *
 * When using aggregated feature summary from a previous session, vectors of different dimensions
 * are transformed to zero vectors and [[FeatureRejection.WrongDimension]] rejections are reported.
 */
object PolynomialExpansion extends SettingsBuilder {

  /**
   * Create a new [[PolynomialExpansion]] instance.
   * @param degree the polynomial degree to expand, which should be greater than or equal to 1
   * @param expectedLength expected length of the input vectors, or 0 to infer from data
   */
  def apply(name: String,
            degree: Int = 2,
            expectedLength: Int = 0): Transformer[Array[Double], Int, Int] =
    new PolynomialExpansion(name, degree, expectedLength)

  /**
   * Create a new [[PolynomialExpansion]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Array[Double], Int, Int] = {
    val degree = setting.params("degree").toInt
    val expectedLength = setting.params("expectedLength").toInt
    PolynomialExpansion(setting.name, degree, expectedLength)
  }

  def expand(v: Array[Double], degree: Int): Array[Double] = {
    val n = v.length
    val polySize = getPolySize(n, degree)
    val polyValues = new Array[Double](polySize - 1)
    expandDense(v, n - 1, degree, 1.0, polyValues, -1)
    polyValues
  }

  private def getPolySize(numFeatures: Int, degree: Int): Int = {
    val n = CombinatoricsUtils.binomialCoefficient(numFeatures + degree, degree)
    // See: https://stackoverflow.com/questions/3038392/do-java-arrays-have-a-maximum-size
    require(n <= Integer.MAX_VALUE - 8)
    n.toInt
  }

  private def expandDense(values: Array[Double],
                          lastIdx: Int,
                          degree: Int,
                          multiplier: Double,
                          polyValues: Array[Double],
                          curPolyIdx: Int): Int = {
    if (multiplier == 0.0) {
      // do nothing
    } else if (degree == 0 || lastIdx < 0) {
      if (curPolyIdx >= 0) { // skip the very first 1
        polyValues(curPolyIdx) = multiplier
      }
    } else {
      val v = values(lastIdx)
      val lastIdx1 = lastIdx - 1
      var alpha = multiplier
      var i = 0
      var curStart = curPolyIdx
      while (i <= degree && alpha != 0.0) {
        curStart = expandDense(values, lastIdx1, degree - i, alpha, polyValues, curStart)
        i += 1
        alpha *= v
      }
    }
    curPolyIdx + getPolySize(lastIdx + 1, degree)
  }
}

private[featran] class PolynomialExpansion(name: String, val degree: Int, val expectedLength: Int)
    extends Transformer[Array[Double], Int, Int](name) {
  require(degree >= 1, "degree must be >= 1")
  override val aggregator: Aggregator[Array[Double], Int, Int] =
    Aggregators.seqLength(expectedLength)
  override def featureDimension(c: Int): Int =
    PolynomialExpansion.getPolySize(c, degree) - 1
  override def featureNames(c: Int): Seq[String] = names(featureDimension(c))
  override def buildFeatures(a: Option[Array[Double]], c: Int, fb: FeatureBuilder[_]): Unit =
    a match {
      case Some(x) =>
        if (x.length != c) {
          fb.skip(featureDimension(c))
          fb.reject(this, FeatureRejection.WrongDimension(c, x.length))
        } else {
          val data = PolynomialExpansion.expand(x, degree)
          fb.add(names(featureDimension(c)), data)
        }
      case None => fb.skip(featureDimension(c))
    }
  override def encodeAggregator(c: Int): String = c.toString
  override def decodeAggregator(s: String): Int = s.toInt
  override def params: Map[String, String] =
    Map("degree" -> degree.toString, "expectedLength" -> expectedLength.toString)

  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readDoubleArray(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[Array[Double]] => fw.IF =
    fw.writeDoubleArray(name)
}

// Ported from commons-math3
private object CombinatoricsUtils {
  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  def binomialCoefficient(n: Int, k: Int): Long = {
    checkBinomial(n, k)
    if (n == k || k == 0) {
      1
    } else if (k == 1 || k == n - 1) {
      n
    } else if (k > n / 2) {
      // Use symmetry for large k
      binomialCoefficient(n, n - k)
    } else {
      // We use the formula
      // (n choose k) = n! / (n-k)! / k!
      // (n choose k) == ((n-k+1)*...*n) / (1*...*k)
      // which could be written
      // (n choose k) == (n-1 choose k-1) * n / k
      var result = 1L
      if (n <= 61) {
        // For n <= 61, the naive implementation cannot overflow.
        var i = n - k + 1
        var j = 1
        while (j <= k) {
          result = result * i / j
          i += 1
          j += 1
        }
      } else if (n <= 66) {
        // For n > 61 but n <= 66, the result cannot overflow,
        // but we must take care not to overflow intermediate values.
        var i = n - k + 1
        var j = 1
        while (j <= k) {
          // We know that (result * i) is divisible by j,
          // but (result * i) may overflow, so we split j:
          // Filter out the gcd, d, so j/d and i/d are integer.
          // result is divisible by (j/d) because (j/d)
          // is relative prime to (i/d) and is a divisor of
          // result * (i/d).
          val d = gcd(i, j)
          result = (result / (j / d)) * (i / d)
          i += 1
          j += 1
        }
      } else {
        // For n > 66, a result overflow might occur, so we check
        // the multiplication, taking care to not overflow
        // unnecessary.
        var i = n - k + 1
        var j = 1
        while (j <= k) {
          val d = gcd(i, j)
          result = mulAndCheck(result / (j / d), i / d)
          i += 1
          j += 1
        }
      }
      result
    }
  }
  // scalastyle:on method.length
  // scalastyle:on cyclomatic.complexity

  def gcd(p: Int, q: Int): Int = {
    if (p == 0 || q == 0) {
      require(p != Int.MinValue && q != Int.MinValue, s"overflow: gcd($p, $q) is 2^31")
      abs(p + q)
    } else {
      var a = p
      var b = q
      var al: Long = a
      var bl: Long = b
      var useLong = false
      if (a < 0) {
        if (a == Int.MinValue) {
          useLong = true
        } else {
          a = -a
        }
        al = -al
      }
      if (b < 0) {
        if (b == Int.MinValue) {
          useLong = true
        } else {
          b = -b
        }
        bl = -bl
      }
      if (useLong) {
        require(al != bl, s"overflow: gcd($p, $q) is 2^31")
      }
      var blbu = bl
      bl = al
      al = blbu % al
      if (al == 0) {
        require(bl <= Int.MaxValue, s"overflow: gcd($p, $q) is 2^31")
        bl.toInt
      } else {
        blbu = bl

        // Now "al" and "bl" fit in an "int".
        b = al.toInt
        a = (blbu % al).toInt
        gcdPositive(a, b)
      }
    }
  }

  private def gcdPositive(p: Int, q: Int): Int = {
    // assert q != 0
    if (p == 0) {
      q
    } else {
      var a = p
      var b = q
      val aTwos = Integer.numberOfTrailingZeros(a)
      a = a >> aTwos
      val bTwos = Integer.numberOfTrailingZeros(b)
      b = b >> bTwos
      val shift = if (aTwos <= bTwos) aTwos else bTwos
      // "a" and "b" are positive.
      // If a > b then "gdc(a, b)" is equal to "gcd(a - b, b)".
      // If a < b then "gcd(a, b)" is equal to "gcd(b - a, a)".
      // Hence, in the successive iterations:
      //  "a" becomes the absolute difference of the current values,
      //  "b" becomes the minimum of the current values.
      while (a != b) {
        val delta = a - b
        b = Math.min(a, b)
        a = Math.abs(delta)

        // Remove any power of 2 in "a" ("b" is guaranteed to be odd).
        a >>= Integer.numberOfTrailingZeros(a)
      }

      // Recover the common power of 2.
      a << shift
    }
  }

  def mulAndCheck(a: Long, b: Long): Long = {
    if (a > b) {
      // use symmetry to reduce boundary cases
      mulAndCheck(b, a)
    } else {
      if (a < 0) {
        if (b < 0) {
          // check for positive overflow with negative a, negative b
          require(a >= Long.MaxValue / b)
          a * b
        } else if (b > 0) {
          // check for negative overflow with negative a, positive b
          require(a >= Long.MinValue / b)
          a * b
        } else {
          // assert b == 0
          0
        }
      } else if (a > 0) {
        // assert a > 0
        // assert b > 0

        // check for positive overflow with positive a, positive b
        require(a <= Long.MaxValue / b)
        a * b
      } else {
        // assert a == 0
        0
      }
    }
  }

  @inline
  def abs(x: Int): Int = (x ^ (~(x >>> 31) + 1)) + (x >>> 31)

  private def checkBinomial(n: Int, k: Int): Unit = {
    require(n >= k, s"must have n >= k for binomial coefficient (n, k), got k = $k, n = $n")
    require(n >= 0, s"must have n >= 0 for binomial coefficient (n, k), got n = $n")
  }
}
