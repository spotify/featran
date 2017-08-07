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

import scala.collection.SortedMap

object HashNHotEncoder {
  def apply(name: String, hashBucketSize: Int = 0): Transformer[Seq[String], HLL, Int] =
    new HashNHotEncoder(name, hashBucketSize)
}

private class HashNHotEncoder(name: String, hashBucketSize: Int)
  extends BaseHashHotEncoder[Seq[String]](name, hashBucketSize) {
  override def prepare(a: Seq[String]): HLL = a.map(hllMonoid.toHLL(_)).reduce(hllMonoid.plus)

  override def buildFeatures(a: Option[Seq[String]], c: Int, fb: FeatureBuilder[_]): Unit = {
    fb.init(c)
    a match {
      case Some(xs) => {
        var prev = -1
          SortedMap(xs.map(i => (HashEncoder.bucket(i, c), i)): _*)
          .foreach { v =>
            val (curr, key) = v
            val gap = curr - prev - 1
            if (gap > 0) fb.skip(gap)
            fb.add(name + '_' + key, 1.0)
            prev = curr
          }
        val gap = c - prev - 1
        if (gap > 0) fb.skip(gap)
      }
      case None => fb.skip(c)
    }
  }
}

