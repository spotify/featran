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
            max: Double = 1.0): Transformer[Double, (Min[Double], Max[Double]), C] =
    new MinMaxScaler(name, min, max)

  private type C = (Double, Double, Double)
}

private class MinMaxScaler(name: String, val min: Double, val max: Double)
  extends OneDimensional[Double, (Min[Double], Max[Double]), MinMaxScaler.C](name) {
  require(max > min, s"max must be > min")

  import MinMaxScaler.C

  override val aggregator: Aggregator[Double, (Min[Double], Max[Double]), C] =
    Aggregators.from[Double](x => (Min(x), Max(x))).to { r =>
      val (aMin, aMax) = (r._1.get, r._2.get)
      val f = if ((aMax - aMin).isInfinity) 2.0 else 1.0 // scaling factor to avoid overflow
      (aMin / f, aMax / f, f)
    }

  override def buildFeatures(a: Option[Double], c: C, fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      val (aMin, aMax, f) = c
      val truncated = math.max(math.min(x / f, aMax), aMin)
      fb.add(name, (truncated - aMin) / (aMax - aMin) * (max - min) + min)
    case None => fb.add(name, min)
  }

  override def encodeAggregator(c: Option[C]): Option[String] =
    c.map(t => s"${t._1},${t._2},${t._3}")
  override def decodeAggregator(s: Option[String]): Option[C] =
    s.map { x =>
      val t = x.split(",")
      (t(0).toDouble, t(1).toDouble, t(2).toDouble)
    }
  override def params: Map[String, String] = Map("min" -> min.toString, "max" -> max.toString)
}
