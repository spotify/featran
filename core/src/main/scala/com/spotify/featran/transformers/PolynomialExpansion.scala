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

import com.spotify.featran.FeatureBuilder
import com.twitter.algebird.Aggregator
import org.apache.commons.math3.util.CombinatoricsUtils

object PolynomialExpansion {
  def apply(name: String, degree: Int = 2): Transformer[Array[Double], Int, Int] =
    new PolynomialExpansion(name, degree)

  def expand(v: Array[Double], degree: Int): Array[Double] = {
    val n = v.length
    val polySize = getPolySize(n, degree)
    val polyValues = new Array[Double](polySize - 1)
    expandDense(v, n - 1, degree, 1.0, polyValues, -1)
    polyValues
  }

  private def getPolySize(numFeatures: Int, degree: Int): Int = {
    val n = CombinatoricsUtils.binomialCoefficient(numFeatures + degree, degree)
    require(n <= Integer.MAX_VALUE)
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

private class PolynomialExpansion(name: String, val degree: Int)
  extends Transformer[Array[Double], Int, Int](name) {
  require(degree >= 1, "degree must be >= 1")
  override val aggregator: Aggregator[Array[Double], Int, Int] = Aggregators.arrayLength
  override def featureDimension(c: Int): Int = PolynomialExpansion.getPolySize(c, degree) - 1
  override def featureNames(c: Int): Seq[String] = (1 to featureDimension(c)).map(name + "_" + _)
  override def buildFeatures(a: Option[Array[Double]], c: Int,
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) => PolynomialExpansion.expand(x, degree).foreach(fb.add)
    case None => (1 to featureDimension(c)).foreach(_ => fb.skip())
  }
}
