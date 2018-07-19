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

import com.spotify.featran.{FlatReader, FlatWriter}

/**
 * Transform features by passing them through.
 *
 * Missing values are transformed to 0.0.
 */
object Identity extends SettingsBuilder {

  /**
   * Create a new [[Identity]] instance.
   */
  def apply(name: String): Transformer[Double, Unit, Unit] = new Identity(name)

  /**
   * Create a new [[Identity]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Double, Unit, Unit] =
    Identity(setting.name)
}

private[featran] class Identity(name: String) extends MapOne[Double](name) {
  override def map(a: Double): Double = a
  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readDouble(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[Double] => fw.IF =
    fw.writeDouble(name)
}
