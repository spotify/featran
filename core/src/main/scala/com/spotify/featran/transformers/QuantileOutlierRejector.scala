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
import com.twitter.algebird._

/**
 * Reject values in the first and/or last quantiles defined by the number of buckets in the
 * `numBuckets` parameter.
 *
 * The bin ranges are chosen using the Algebird's QTree approximate data structure. The precision
 * of the approximation can be controlled with the `k` parameter.
 *
 * All values are transformed to zeros.
 *
 * Values in the first and/or last quantiles are rejected as [[FeatureRejection.Outlier]].
 *
 * When using aggregated feature summary from a previous session, values outside of previously
 * seen `[min, max]` will also report [[FeatureRejection.Outlier]] as rejection.
 */
object QuantileOutlierRejector extends SettingsBuilder {
  import BaseQuantileRejector._

  /**
   * Create a new [[QuantileOutlierRejector]] instance.
   *
   * @param rejectLower whether to reject outliers in the first quantile
   * @param rejectUpper whether to reject outliers in the last quantile
   * @param numBuckets  number of buckets (quantiles, or categories) into which data points are
   *                    grouped, must be greater than or equal to 2
   * @param k           precision of the underlying Algebird QTree approximation
   */
  def apply(name: String,
            rejectLower: Boolean = true,
            rejectUpper: Boolean = true,
            numBuckets: Int = 3,
            k: Int = QTreeAggregator.DefaultK)
    : Transformer[Double, BaseQuantileRejector.B, BaseQuantileRejector.C] =
    new QuantileOutlierRejector(name, rejectLower, rejectUpper, numBuckets, k)

  /**
   * Create a new [[QuantileOutlierRejector]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Double, B, C] =
    QuantileOutlierRejector(setting.name)
}

private class QuantileOutlierRejector(name: String,
                                      val rejectLower: Boolean,
                                      val rejectUpper: Boolean,
                                      numBuckets: Int,
                                      k: Int)
    extends BaseQuantileRejector(name, numBuckets, k) {
  require(rejectLower || rejectUpper, "at least one of [rejectLower, rejectLower] must be set")

  import BaseQuantileRejector.C

  protected def calculateBounds(fq: Double, lq: Double): (Double, Double) =
    (fq, lq)

  // scalastyle:off cyclomatic.complexity
  override def buildFeatures(a: Option[Double], c: C, fb: FeatureBuilder[_]): Unit = {
    // we always skip since we don't care about the actual value, just the rejections
    fb.skip()
    a.foreach { x =>
      val (fq, lq, min, max) = c
      val (l, u) = calculateBounds(fq, lq)
      // all elements can't be the same
      if (x < min || x > max) {
        fb.reject(this, FeatureRejection.Outlier(x))
      } else if (min < max) {
        val r = (rejectLower, rejectUpper) match {
          case (true, true)  => x < l || x > u
          case (true, false) => x < l
          case (false, true) => x > u
          case _             => false
        }
        if (r) fb.reject(this, FeatureRejection.Outlier(x))
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  override def params: Map[String, String] =
    super.params ++ Map(
      "rejectLower" -> rejectLower.toString,
      "rejectUpper" -> rejectUpper.toString
    )
}

private[transformers] object BaseQuantileRejector {
  type B = (QTree[Double], Min[Double], Max[Double])
  type C = (Double, Double, Double, Double)
}

private abstract class BaseQuantileRejector(name: String, val numBuckets: Int, val k: Int)
    extends OneDimensional[Double, BaseQuantileRejector.B, BaseQuantileRejector.C](name) {
  require(numBuckets >= 3, "numBuckets must be >= 3")

  import BaseQuantileRejector.{B, C}

  implicit val sg = new QTreeSemigroup[Double](k)

  override val aggregator: Aggregator[Double, B, C] =
    Aggregators.from[Double](x => (QTree(x), Min(x), Max(x))).to {
      case (qt, min, max) =>
        val lq = (numBuckets - 1.0) / numBuckets
        val fq = 1.0 / numBuckets
        val (u, _) = qt.quantileBounds(lq)
        val (_, l) = qt.quantileBounds(fq)
        (l, u, min.get, max.get)
    }

  override def encodeAggregator(c: (Double, Double, Double, Double)): String =
    s"${c._1},${c._2},${c._3}, ${c._4}"

  override def decodeAggregator(s: String): (Double, Double, Double, Double) = {
    val a = s.split(",")
    (a(0).toDouble, a(1).toDouble, a(2).toDouble, a(3).toDouble)
  }

  override def params: Map[String, String] =
    Map("numBuckets" -> numBuckets.toString, "k" -> k.toString)

  def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readDouble(name)
  def flatWriter[T](implicit fw: FlatWriter[T]): Option[Double] => fw.IF =
    fw.writeDouble(name)
}
