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

import com.spotify.featran.{FeatureBuilder, FeatureRejection}

import scala.collection.SortedMap
import scala.collection.mutable.{Set => MSet}

/**
 * Transform a collection of categorical features to binary columns, with at most N one-values.
 *
 * Missing values are either transformed to zero vectors or encoded as a missing value.
 *
 * When using aggregated feature summary from a previous session, unseen labels are either
 * transformed to zero vectors or encoded as `__unknown__` (if `encodeMissingValue` is true) and
 * [FeatureRejection.Unseen]] rejections are reported.
 */
object NHotEncoder {

  /**
   * Create a new [[NHotEncoder]] instance.
   */
  def apply(name: String, encodeMissingValue: Boolean = false)
    : Transformer[Seq[String], Set[String], SortedMap[String, Int]] =
    new NHotEncoder(name, encodeMissingValue)
}

private class NHotEncoder(name: String, encodeMissingValue: Boolean)
    extends BaseHotEncoder[Seq[String]](name, encodeMissingValue) {

  import MissingValue.missingValueToken

  override def prepare(a: Seq[String]): Set[String] = Set(a: _*)
  override def buildFeatures(a: Option[Seq[String]],
                             c: SortedMap[String, Int],
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(xs) =>
      val keys = xs.distinct.sorted
      var prev = -1
      val unseen = MSet[String]()
      keys.foreach { key =>
        c.get(key) match {
          case Some(curr) =>
            val gap = curr - prev - 1
            if (gap > 0) fb.skip(gap)
            fb.add(name + '_' + key, 1.0)
            prev = curr
          case None =>
            unseen += key
        }
      }
      val gap = c.size - prev - 1
      if (gap > 0) fb.skip(gap)
      if (encodeMissingValue) {
        if (unseen.isEmpty) {
          fb.skip()
        } else {
          fb.add(name + '_' + missingValueToken, 1.0)
        }
      }
      if (unseen.nonEmpty) {
        fb.reject(this, FeatureRejection.Unseen(unseen.toSet))
      }
    case None => addMissingItem(c, fb)
  }

}
