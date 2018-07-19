/*
 * Copyright 2018 Spotify AB.
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

import scala.collection.SortedMap

/**
 * Transform a collection of categorical features to a single value that is the position
 * of that feature within the complete set of categories.
 *
 * Missing values are transformed to zeros so may collide with the first position. Rejections can
 * be used to remove this case.
 *
 * When using aggregated feature summary from a previous session, unseen labels are ignored and
 * [[FeatureRejection.Unseen]] rejections are reported.
 */
object PositionEncoder extends SettingsBuilder {

  /**
   * Create a new [[PositionEncoder]] instance.
   */
  def apply(name: String): Transformer[String, Set[String], SortedMap[String, Int]] =
    new PositionEncoder(name)

  /**
   * Create a new [[PositionEncoder]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[String, Set[String], SortedMap[String, Int]] =
    PositionEncoder(setting.name)
}

private[featran] class PositionEncoder(name: String) extends BaseHotEncoder[String](name, false) {
  override def prepare(a: String): Set[String] = Set(a)
  override def featureDimension(c: SortedMap[String, Int]): Int = 1
  override def featureNames(c: SortedMap[String, Int]): Seq[String] = Seq(name)
  override def buildFeatures(a: Option[String],
                             c: SortedMap[String, Int],
                             fb: FeatureBuilder[_]): Unit = {
    a match {
      case Some(k) =>
        c.get(k) match {
          case Some(v) => fb.add(name, v.toDouble)
          case None =>
            fb.skip(1)
            fb.reject(this, FeatureRejection.Unseen(Set(k)))
        }
      case None =>
        fb.skip(1)
        fb.reject(this, FeatureRejection.Collision)
    }
  }

  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readString(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[String] => fw.IF =
    fw.writeString(name)
}
