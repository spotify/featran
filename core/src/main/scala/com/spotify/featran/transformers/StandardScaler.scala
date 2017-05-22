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
import com.twitter.algebird.{Aggregator, Moments}

object StandardScaler {
  // Missing value = if (withMean) 0.0 else mean
  def apply(name: String,
            withStd: Boolean = true,
            withMean: Boolean = false): Transformer[Double, Moments, (Double, Double)] =
    new StandardScaler(name, withStd, withMean)
}

private class StandardScaler(name: String, val withStd: Boolean, val withMean: Boolean)
  extends OneDimensional[Double, Moments, (Double, Double)](name) {
  override val aggregator: Aggregator[Double, Moments, (Double, Double)] =
    Aggregators.from[Double](Moments(_)).to(r => (r.mean, r.stddev))
  override def buildFeatures(a: Option[Double], c: (Double, Double),
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      val r = (withStd, withMean) match {
        case (true, true) => (x - c._1) / c._2
        case (true, false) => (x - c._1) / c._2 + c._1
        case (false, true) => x - c._1
        case (false, false) => x
      }
      fb.add(r)
    case None => fb.add(if (withMean) 0.0 else c._1)
  }
}
