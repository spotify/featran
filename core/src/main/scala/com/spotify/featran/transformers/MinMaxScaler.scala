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
  // Missing value = min
  def apply(name: String,
            min: Double = 0.0,
            max: Double = 1.0): Transformer[Double, (Min[Double], Max[Double]), (Double, Double)] =
    new MinMaxScaler(name, min, max)
}

private class MinMaxScaler(name: String, val min: Double, val max: Double)
  extends OneDimensional[Double, (Min[Double], Max[Double]), (Double, Double)](name) {
  override val aggregator: Aggregator[Double, (Min[Double], Max[Double]), (Double, Double)] =
    Aggregators.from[Double](x => (Min(x), Max(x))).to(r => (r._1.get, r._2.get - r._1.get))
  override def buildFeatures(a: Option[Double], c: (Double, Double),
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) => fb.add((x - c._1) / c._2 * (max - min) + min)
    case None => fb.add(min)
  }
}
