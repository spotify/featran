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

import com.spotify.featran.{FeatureBuilder, FeatureRejection, FlatReader, FlatWriter}
import com.twitter.algebird.{Aggregator, Max}

/**
 * Transform features by rescaling each feature to range [-1, 1] by dividing through the maximum
 * absolute value in each feature.
 *
 * Missing values are transformed to 0.0.
 *
 * When using aggregated feature summary from a previous session, out of bound values are
 * truncated to -1.0 or 1.0 and [[FeatureRejection.OutOfBound]] rejections are reported.
 */
object MaxAbsScaler extends SettingsBuilder {

  /**
   * Create a new [[MaxAbsScaler]] instance.
   */
  def apply(name: String): Transformer[Double, Max[Double], Double] =
    new MaxAbsScaler(name)

  /**
   * Create a new [[MaxAbsScaler]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Double, Max[Double], Double] =
    MaxAbsScaler(setting.name)
}

private[featran] class MaxAbsScaler(name: String)
    extends OneDimensional[Double, Max[Double], Double](name) {
  override val aggregator: Aggregator[Double, Max[Double], Double] =
    Aggregators.from[Double](x => Max(math.abs(x))).to(_.get)
  override def buildFeatures(a: Option[Double], c: Double, fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      // truncate x to [-max, max]
      val truncated = math.min(math.abs(x), c) * math.signum(x)
      fb.add(name, truncated / c)
      if (math.abs(x) > c) {
        fb.reject(this, FeatureRejection.OutOfBound(-c, c, x))
      }
    case None => fb.skip()
  }
  override def encodeAggregator(c: Double): String = c.toString
  override def decodeAggregator(s: String): Double = s.toDouble
  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readDouble(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[Double] => fw.IF =
    fw.writeDouble(name)
}
