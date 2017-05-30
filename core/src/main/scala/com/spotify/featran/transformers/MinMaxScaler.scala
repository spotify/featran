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
import com.twitter.algebird.{Aggregator, Max, Min}

object MinMaxScaler {
  /**
   * Transform features by rescaling each feature to a specific range [`min`, `max`] (default
   * [0, 1]).
   *
   * Missing values are transformed to `min`.
   *
   * When using aggregated feature summary from a previous session, out of bound values are
   * truncated to `min` or `max`.
   */
  def apply(name: String,
            min: Double = 0.0,
            max: Double = 1.0): Transformer[Double, (Min[Double], Max[Double]), (Double, Double)] =
    new MinMaxScaler(name, min, max)
}

private class MinMaxScaler(name: String, val min: Double, val max: Double)
  extends OneDimensional[Double, (Min[Double], Max[Double]), (Double, Double)](name) {
  require(max > min, "max must be > min")

  override val aggregator: Aggregator[Double, (Min[Double], Max[Double]), (Double, Double)] =
    Aggregators.from[Double](x => (Min(x), Max(x))).to(r => (r._1.get, r._2.get))
  override def buildFeatures(a: Option[Double], c: (Double, Double),
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      val truncated = math.max(math.min(x, c._2), c._1)
      fb.add((truncated - c._1) / (c._2 - c._1) * (max - min) + min)
    case None => fb.add(min)
  }

  override def encodeAggregator(c: Option[(Double, Double)]): Option[String] =
    c.map(t => s"${t._1},${t._2}")
  override def decodeAggregator(s: Option[String]): Option[(Double, Double)] =
    s.map { x =>
      val t = x.split(",")
      (t(0).toDouble, t(1).toDouble)
    }
  override def params: Map[String, String] = Map("min" -> min.toString, "max" -> max.toString)
}
