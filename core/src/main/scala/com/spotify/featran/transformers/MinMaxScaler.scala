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
import com.twitter.algebird.{Aggregator, Max, Min}

import scala.collection.SortedMap

/**
 * Transform features by rescaling each feature to a specific range [`min`, `max`] (default
 * [0, 1]).
 *
 * Missing values are transformed to `min`.
 *
 * When using aggregated feature summary from a previous session, out of bound values are
 * truncated to `min` or `max` and [[FeatureRejection.OutOfBound]] rejections are reported.
 */
object MinMaxScaler extends SettingsBuilder {

  /**
   * Create a new [[MinMaxScaler]] instance.
   * @param min lower bound after transformation, shared by all features
   * @param max upper bound after transformation, shared by all features
   */
  def apply(name: String,
            min: Double = 0.0,
            max: Double = 1.0): Transformer[Double, (Min[Double], Max[Double]), C] =
    new MinMaxScaler(name, min, max)

  /**
   * Create a new [[MinMaxScaler]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Double, (Min[Double], Max[Double]), C] = {
    val min = setting.params("min").toDouble
    val max = setting.params("max").toDouble
    MinMaxScaler(setting.name, min, max)
  }

  private type C = (Double, Double, Double)
}

private[featran] class MinMaxScaler(name: String, val min: Double, val max: Double)
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
      if (x < aMin || x > aMax) {
        fb.reject(this, FeatureRejection.OutOfBound(aMin, aMax, x))
      }
    case None => fb.add(name, min)
  }

  override def encodeAggregator(c: C): String = s"${c._1},${c._2},${c._3}"
  override def decodeAggregator(s: String): C = {
    val t = s.split(",")
    (t(0).toDouble, t(1).toDouble, t(2).toDouble)
  }
  override def params: Map[String, String] =
    Map("min" -> min.toString, "max" -> max.toString)

  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readDouble(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[Double] => fw.IF =
    fw.writeDouble(name)
}
