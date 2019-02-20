/*
 * Copyright 2019 Spotify AB.
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
import com.twitter.algebird.Aggregator

/**
 * Transform an optional 1D feature to an indicator variable indicating presence.
 *
 * Missing values are mapped to 0.0. Present values are mapped to 1.0.
 */
object Indicator extends SettingsBuilder {

  /**
   * Create a new [[Indicator]] instance.
   * @param threshold threshold to binarize continuous features
   */
  def apply(name: String): Transformer[Double, Unit, Unit] =
    new Indicator(name)

  /**
   * Create a new [[Indicator]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Double, Unit, Unit] =
    Indicator(setting.name)
}

private[featran] class Indicator(name: String) extends MapOne[Double](name) {

  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readDouble(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[Double] => fw.IF =
    fw.writeDouble(name)

  override def map(a: Double): Double = 1

}
