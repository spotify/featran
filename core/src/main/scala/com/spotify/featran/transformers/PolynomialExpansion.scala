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

import com.spotify.featran.{FeatureBuilder, FeatureRejection}
import com.twitter.algebird.Aggregator
import org.apache.commons.math3.util.CombinatoricsUtils

/**
 * Transform vector features by expanding them into a polynomial space, which is formulated by an
 * n-degree combination of original dimensions.
 *
 * Missing values are transformed to zero vectors.
 *
 * When using aggregated feature summary from a previous session, vectors of different dimensions
 * are transformed to zero vectors and [[FeatureRejection.WrongDimension]] rejections are reported.
 */
object PolynomialExpansion {
  /**
   * Create a new [[PolynomialExpansion]] instance.
   * @param degree the polynomial degree to expand, which should be greater than or equal to 1
   * @param expectedLength expected length of the input vectors, or 0 to infer from data
   */
  def apply(name: String, degree: Int = 2, expectedLength: Int = 0)
  : Transformer[Array[Double], Int, Int] = new PolynomialExpansion(name, degree, expectedLength)

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

private class PolynomialExpansion(name: String, val degree: Int, val expectedLength: Int)
  extends Transformer[Array[Double], Int, Int](name) {
  require(degree >= 1, "degree must be >= 1")
  override val aggregator: Aggregator[Array[Double], Int, Int] =
    Aggregators.seqLength(expectedLength)
  override def featureDimension(c: Int): Int = PolynomialExpansion.getPolySize(c, degree) - 1
  override def featureNames(c: Int): Seq[String] = names(featureDimension(c))
  override def buildFeatures(a: Option[Array[Double]], c: Int,
                             fb: FeatureBuilder[_]): Unit = a match {
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
  override def encodeAggregator(c: Option[Int]): Option[String] = c.map(_.toString)
  override def decodeAggregator(s: Option[String]): Option[Int] = s.map(_.toInt)
  override def params: Map[String, String] = Map(
    "degree" -> degree.toString,
    "expectedLength" -> expectedLength.toString)
}
