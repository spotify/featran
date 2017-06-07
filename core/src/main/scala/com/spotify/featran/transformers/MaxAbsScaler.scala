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
import com.twitter.algebird.{Aggregator, Max}

object MaxAbsScaler {
  /**
   * Transform features by rescaling each feature to range [-1, 1] by dividing through the maximum
   * absolute value in each feature.
   *
   * Missing values are transformed to 0.0.
   *
   * When using aggregated feature summary from a previous session, out of bound values are
   * truncated to -1.0 or 1.0.
   */
  def apply(name: String): Transformer[Double, Max[Double], Double] = new MaxAbsScaler(name)
}

private class MaxAbsScaler(name: String) extends OneDimensional[Double, Max[Double], Double](name) {
  override val aggregator: Aggregator[Double, Max[Double], Double] =
    Aggregators.from[Double](x => Max(math.abs(x))).to(_.get)
  override def buildFeatures(a: Option[Double], c: Double, fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      // truncate x to [-max, max]
      val truncated = math.min(math.abs(x), c) * math.signum(x)
      fb.add(name, truncated / c)
    case None => fb.skip()
  }
  override def encodeAggregator(c: Option[Double]): Option[String] = c.map(_.toString)
  override def decodeAggregator(s: Option[String]): Option[Double] = s.map(_.toDouble)
}
