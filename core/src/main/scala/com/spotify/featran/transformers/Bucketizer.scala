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

import java.util.{TreeMap => JTreeMap}

import com.spotify.featran.{FeatureBuilder, FeatureRejection, FlatReader, FlatWriter}
import com.twitter.algebird.{Aggregator, HLL}

/**
 * Transform a column of continuous features to n columns of feature buckets.
 *
 * With n+1 splits, there are n buckets. A bucket defined by splits x,y holds values in the range
 * [x,y) except the last bucket, which also includes y. Splits should be strictly increasing.
 * Values at -inf, inf must be explicitly provided to cover all double values; Otherwise,
 * [[FeatureRejection.OutOfBound]] rejection will be reported for values outside the splits
 * specified.. Two examples of splits are
 * `Array(Double.NegativeInfinity, 0.0, 1.0, Double.PositiveInfinity)` and `Array(0.0, 1.0, 2.0)`.
 *
 * Note that if you have no idea of the upper and lower bounds of the targeted column, you should
 * add `Double.NegativeInfinity` and `Double.PositiveInfinity` as the bounds of your splits to
 * prevent a potential [[FeatureRejection.OutOfBound]] rejection.
 *
 * Note also that the splits that you provided have to be in strictly increasing order, i.e.
 * `s0 < s1 < s2 < ... < sn`.
 *
 * Missing values are transformed to zero vectors.
 */
object Bucketizer extends SettingsBuilder {

  /**
   * Create a new [[Bucketizer]] instance.
   * @param splits parameter for mapping continuous features into buckets
   */
  def apply(name: String, splits: Array[Double]): Transformer[Double, Unit, Unit] =
    new Bucketizer(name, splits)

  /**
   * Create a new [[Bucketizer]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Double, Unit, Unit] = {
    val params = setting.params
    val str = params("splits")
    val splits = str.slice(1, str.length - 1).split(",").map(_.toDouble).sorted
    Bucketizer(setting.name, splits)
  }
}

private[featran] class Bucketizer(name: String, val splits: Array[Double])
    extends Transformer[Double, Unit, Unit](name) {
  require(splits.length >= 3, "splits.length must be >= 3")
  private val lower = splits.head
  private val upper = splits.last
  private val map = {
    val m = new JTreeMap[Double, Int]()
    var i = 1
    while (i < splits.length) {
      require(splits(i) > splits(i - 1), "splits must be in increasing order")
      m.put(splits(i), i - 1)
      i += 1
    }
    m
  }
  override val aggregator: Aggregator[Double, Unit, Unit] =
    Aggregators.unit[Double]
  override def featureDimension(c: Unit): Int = splits.length - 1
  override def featureNames(c: Unit): Seq[String] = names(splits.length - 1)
  override def buildFeatures(a: Option[Double], c: Unit, fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      if (x < lower || x > upper) {
        fb.skip(splits.length - 1)
        fb.reject(this, FeatureRejection.OutOfBound(lower, upper, x))
      } else {
        val e = map.higherEntry(x)
        val offset = if (e != null) e.getValue else splits.length - 2
        fb.skip(offset)
        fb.add(nameAt(offset), 1.0)
        fb.skip(splits.length - 2 - offset)
      }
    case None => fb.skip(splits.length - 1)
  }

  override def encodeAggregator(c: Unit): String = ""
  override def decodeAggregator(s: String): Unit = ()
  override def params: Map[String, String] =
    Map("splits" -> splits.mkString("[", ",", "]"))

  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readDouble(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[Double] => fw.IF =
    fw.writeDouble(name)
}
