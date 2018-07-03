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

import com.spotify.featran.FeatureRejection
import com.twitter.algebird.QTreeAggregator

/**
 * Reject values if they fall outside of either `factor * IQR` below the first quartile or
 * `factor * IQR` above the third quartile.
 *
 * IQR or inter quartile range is the range between the first and the third quartiles.
 *
 * The bin ranges are chosen using the Algebird's QTree approximate data structure. The precision
 * of the approximation can be controlled with the `k` parameter.
 *
 * All values are transformed to zeros.
 *
 * Values `factor * IQR` below the first quartile or `factor * IQR` above the third quartile are
 * rejected as [[FeatureRejection.Outlier]].
 *
 * When using aggregated feature summary from a previous session, values outside of previously
 * seen `[min, max]` will also report [[FeatureRejection.Outlier]] as rejection.
 */
object IQROutlierRejector extends SettingsBuilder {
  import BaseQuantileRejector._
  private val DefaultFactor = 1.5

  /**
   * Create a new [[IQROutlierRejector]] instance.
   *
   * @param rejectLower whether to reject outliers `factor` * IQR below the first quartile
   * @param rejectUpper whether to reject outliers `factor` * IQR above the third quartile
   * @param k           precision of the underlying Algebird QTree approximation
   */
  def apply(name: String,
            rejectLower: Boolean = true,
            rejectUpper: Boolean = true,
            k: Int = QTreeAggregator.DefaultK,
            factor: Double = DefaultFactor)
    : Transformer[Double, BaseQuantileRejector.B, BaseQuantileRejector.C] =
    new IQROutlierRejector(name, rejectLower, rejectUpper, k, factor)

  /**
   * Create a new [[IQROutlierRejector]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Double, B, C] =
    IQROutlierRejector(setting.name)
}

private class IQROutlierRejector(name: String,
                                 rejectLower: Boolean,
                                 rejectUpper: Boolean,
                                 k: Int,
                                 val factor: Double)
    extends QuantileOutlierRejector(name, rejectLower, rejectUpper, 4, k) {

  override def calculateBounds(fq: Double, lq: Double): (Double, Double) = {
    val iqr = lq - fq
    val l = fq - (iqr * factor)
    val u = lq - (iqr * factor)
    (l, u)
  }

  override def params: Map[String, String] =
    super.params ++ Map("factor" -> factor.toString)
}
