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

import com.spotify.featran.{FeatureBuilder, FlatReader, FlatWriter}
import com.twitter.algebird.HLL

import scala.collection.SortedSet

/**
 * Transform a collection of categorical features to binary columns, with at most N one-values.
 * Similar to [[NHotEncoder]] but uses MurmursHash3 to hash features into buckets to reduce CPU
 * and memory overhead.
 *
 * Missing values are transformed to zero vectors.
 *
 * If hashBucketSize is inferred with HLL, the estimate is scaled by sizeScalingFactor to reduce
 * the number of collisions.
 *
 * Rough table of relationship of scaling factor to % collisions, measured from a corpus of 466544
 * English words:
 *
 * {{{
 * sizeScalingFactor     % Collisions
 * -----------------     ------------
 *                 2     17.9934%
 *                 4     10.5686%
 *                 8      5.7236%
 *                16      3.0019%
 *                32      1.5313%
 *                64      0.7864%
 *               128      0.3920%
 *               256      0.1998%
 *               512      0.0975%
 *              1024      0.0478%
 *              2048      0.0236%
 *              4096      0.0071%
 * }}}
 */
object HashNHotEncoder extends SettingsBuilder {

  /**
   * Create a new [[HashNHotEncoder]] instance.
   * @param hashBucketSize number of buckets, or 0 to infer from data with HyperLogLog
   * @param sizeScalingFactor when hashBucketSize is 0, scale HLL estimate by this amount
   */
  def apply(name: String,
            hashBucketSize: Int = 0,
            sizeScalingFactor: Double = 8.0): Transformer[Seq[String], HLL, Int] =
    new HashNHotEncoder(name, hashBucketSize, sizeScalingFactor)

  /**
   * Create a new [[HashNHotEncoder]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Seq[String], HLL, Int] = {
    val hashBucketSize = setting.params("hashBucketSize").toInt
    val sizeScalingFactor = setting.params("sizeScalingFactor").toDouble
    HashNHotEncoder(setting.name, hashBucketSize, sizeScalingFactor)
  }
}

private[featran] class HashNHotEncoder(name: String, hashBucketSize: Int, sizeScalingFactor: Double)
    extends BaseHashHotEncoder[Seq[String]](name, hashBucketSize, sizeScalingFactor) {
  override def prepare(a: Seq[String]): HLL =
    a.map(hllMonoid.toHLL(_)).reduce(hllMonoid.plus)

  override def buildFeatures(a: Option[Seq[String]], c: Int, fb: FeatureBuilder[_]): Unit = {
    a match {
      case Some(xs) =>
        var prev = -1
        SortedSet(xs.map(HashEncoder.bucket(_, c)): _*).foreach { curr =>
          val gap = curr - prev - 1
          if (gap > 0) fb.skip(gap)
          fb.add(name + '_' + curr, 1.0)
          prev = curr
        }
        val gap = c - prev - 1
        if (gap > 0) fb.skip(gap)
      case None => fb.skip(c)
    }
  }

  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readStrings(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[Seq[String]] => fw.IF =
    fw.writeStrings(name)
}
