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
import com.twitter.algebird.HLL

import scala.collection.SortedSet

object HashNHotEncoder {
  /**
    * Transform a collection of categorical features to binary columns, with at most N one-values.
    * Similar to [[NHotEncoder]] but uses MurmursHash3 to hash features into buckets
    * to reduce CPU and memory overhead.
    *
    * Missing values are transformed to [0.0, 0.0, ...].
    *
    * @param hashBucketSize number of buckets, or 0 to infer from data with HyperLogLog
    */
  def apply(name: String, hashBucketSize: Int = 0): Transformer[Seq[String], HLL, Int] =
    new HashNHotEncoder(name, hashBucketSize)
}

private class HashNHotEncoder(name: String, hashBucketSize: Int)
  extends BaseHashHotEncoder[Seq[String]](name, hashBucketSize) {
  override def prepare(a: Seq[String]): HLL = a.map(hllMonoid.toHLL(_)).reduce(hllMonoid.plus)

  override def buildFeatures(a: Option[Seq[String]], c: Int, fb: FeatureBuilder[_]): Unit = {
    fb.init(c)
    a match {
      case Some(xs) =>
        var prev = -1
        SortedSet(xs.map(HashEncoder.bucket(_, c)): _*)
          .foreach { curr =>
            val gap = curr - prev - 1
            if (gap > 0) fb.skip(gap)
            fb.add("$name_$i", 1.0)
            prev = curr
          }
        val gap = c - prev - 1
        if (gap > 0) fb.skip(gap)
      case None => fb.skip(c)
    }
  }
}
