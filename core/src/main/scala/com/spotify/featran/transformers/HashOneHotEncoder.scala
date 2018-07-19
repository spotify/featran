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
import com.twitter.algebird._

import scala.math.ceil
import scala.util.hashing.MurmurHash3

/**
 * Transform a collection of categorical features to binary columns, with at most a single
 * one-value. Similar to [[OneHotEncoder]] but uses MurmursHash3 to hash features into buckets to
 * reduce CPU and memory overhead.
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
object HashOneHotEncoder extends SettingsBuilder {

  /**
   * Create a new [[HashOneHotEncoder]] instance.
   * @param hashBucketSize number of buckets, or 0 to infer from data with HyperLogLog
   * @param sizeScalingFactor when hashBucketSize is 0, scale HLL estimate by this amount
   */
  def apply(name: String,
            hashBucketSize: Int = 0,
            sizeScalingFactor: Double = 8.0): Transformer[String, HLL, Int] =
    new HashOneHotEncoder(name, hashBucketSize, sizeScalingFactor)

  /**
   * Create a new [[HashOneHotEncoder]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[String, HLL, Int] = {
    val hashBucketSize = setting.params("hashBucketSize").toInt
    val sizeScalingFactor = setting.params("sizeScalingFactor").toDouble
    HashOneHotEncoder(setting.name, hashBucketSize, sizeScalingFactor)
  }
}

private[featran] class HashOneHotEncoder(name: String,
                                         hashBucketSize: Int,
                                         sizeScalingFactor: Double)
    extends BaseHashHotEncoder[String](name, hashBucketSize, sizeScalingFactor) {
  override def prepare(a: String): HLL = hllMonoid.toHLL(a)

  override def buildFeatures(a: Option[String], c: Int, fb: FeatureBuilder[_]): Unit = {
    a match {
      case Some(x) =>
        val i = HashEncoder.bucket(x, c)
        fb.skip(i)
        fb.add(name + '_' + i, 1.0)
        fb.skip(math.max(0, c - i - 1))
      case None =>
        fb.skip(c)
    }
  }

  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readString(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[String] => fw.IF =
    fw.writeString(name)
}

private[featran] abstract class BaseHashHotEncoder[A](name: String,
                                                      val hashBucketSize: Int,
                                                      val sizeScalingFactor: Double)
    extends Transformer[A, HLL, Int](name) {
  require(hashBucketSize >= 0, "hashBucketSize must be >= 0")
  require(sizeScalingFactor >= 1.0, "hashBucketSize must be >= 1.0")

  private val hllBits = 12
  implicit protected val hllMonoid = new HyperLogLogMonoid(hllBits)

  def prepare(a: A): HLL

  override val aggregator: Aggregator[A, HLL, Int] =
    if (hashBucketSize == 0) {
      Aggregators.from[A](prepare).to(r => ceil(r.estimatedSize * sizeScalingFactor).toInt)
    } else {
      // dummy aggregator
      new Aggregator[A, HLL, Int] {
        override def prepare(input: A): HLL = SparseHLL(4, Map.empty)
        override def semigroup: Semigroup[HLL] =
          Semigroup.from[HLL]((x, _) => x)
        override def present(reduction: HLL): Int = hashBucketSize
      }
    }
  override def featureDimension(c: Int): Int = c
  override def featureNames(c: Int): Seq[String] = names(c)

  override def encodeAggregator(c: Int): String = c.toString
  override def decodeAggregator(s: String): Int = s.toInt
  override def params: Map[String, String] =
    Map("hashBucketSize" -> hashBucketSize.toString,
        "sizeScalingFactor" -> sizeScalingFactor.toString)
}

private object HashEncoder {
  def bucket(x: String, c: Int): Int = (MurmurHash3.stringHash(x) % c + c) % c
}
