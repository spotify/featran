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

import collection.JavaConverters._
import com.spotify.featran.FeatureBuilder
import com.twitter.algebird.HLL


object HashNHotWeightedEncoder {
  /**
   * Transform a collection of weighted categorical features to columns of weight sums, with at
   * most N values. Similar to [[NHotWeightedEncoder]] but uses MurmursHash3 to hash features into
   * buckets to reduce CPU and memory overhead.
   *
   * Weights of the same labels in a row are summed instead of 1.0 as is the case with the normal
   * [[NHotEncoder]].
   *
    * If hashBucketSize is inferred with HLL, the estimate is scaled by sizeScalingFactor
    * to reduce the number of collisions.
    *
    * Rough table of relationship of scaling factor to % collisions:
    *
    * sizeScalingFactor     % Collisions
    * -----------------     ------------
    * 2048                  0.02%
    * 1024                  0.04%
    * 512                   0.09%
    * 256                   0.18%
    * 128                   0.34%
    * 64                    0.68%
    * 32                    1%
    * 16                    2%
    * 8                     5%
    * 4                     9%
    * 2                     16%
    * 1                     25%
    *
    * @param hashBucketSize number of buckets, or 0 to infer from data with HyperLogLog
    * @param sizeScalingFactor when hashBucketSize is 0, scale HLL estimate by this amount
   */
  def apply(name: String,
            hashBucketSize: Int = 0,
            sizeScalingFactor: Double = 8.0): Transformer[Seq[WeightedLabel], HLL, Int] =
    new HashNHotWeightedEncoder(name, hashBucketSize, sizeScalingFactor)
}

private class HashNHotWeightedEncoder(name: String, hashBucketSize: Int, sizeScalingFactor: Double)
  extends BaseHashHotEncoder[Seq[WeightedLabel]](name, hashBucketSize, sizeScalingFactor) {

  override def prepare(a: Seq[WeightedLabel]): HLL =
    a.map(_.name).map(hllMonoid.toHLL(_)).reduce(hllMonoid.plus)

  override def buildFeatures(a: Option[Seq[WeightedLabel]],
                             c: Int,
                             fb: FeatureBuilder[_]): Unit = {
    fb.init(c)
    a match {
      case Some(xs) =>
        val weights = new java.util.TreeMap[Int,Double]().asScala.withDefaultValue(0.0)
        xs.foreach(x => weights(HashEncoder.bucket(x.name, c)) += x.value)
        var prev = -1
        weights.foreach { v =>
          val (curr, value) = v
          val gap = curr - prev - 1
          if (gap > 0) fb.skip(gap)
          fb.add(name + '_' + curr, value)
          prev = curr
        }
        val gap = c - prev - 1
        if (gap > 0) fb.skip(gap)
      case None => fb.skip(c)
    }
  }
}
