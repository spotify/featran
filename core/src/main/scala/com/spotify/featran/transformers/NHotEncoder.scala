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

import scala.collection.SortedMap

/**
 * Transform a collection of categorical features to binary columns, with at most N one-values.
 *
 * Missing values are transformed to [0.0, 0.0, ...].
 *
 * When using aggregated feature summary from a previous session, unseen labels are ignored.
 */
object NHotEncoder {
  /**
   * Create a new [[NHotEncoder]] instance.
   */
  def apply(name: String): Transformer[Seq[String], Set[String], SortedMap[String, Int]] =
    new NHotEncoder(name)
}

private class NHotEncoder(name: String) extends BaseHotEncoder[Seq[String]](name) {
  override def prepare(a: Seq[String]): Set[String] = Set(a: _*)
  override def buildFeatures(a: Option[Seq[String]],
                             c: SortedMap[String, Int],
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(xs) =>
      val keys = xs.toSet
      val hits = c.filterKeys(keys)
      if (hits.isEmpty) {
        fb.skip(c.size)
      } else {
        var prev = -1
        val it = hits.iterator
        while (it.hasNext) {
          val (key, curr) = it.next()
          val gap = curr - prev - 1
          if (gap > 0) fb.skip(gap)
          fb.add(name + '_' + key, 1.0)
          prev = curr
        }
        val gap = c.size - prev - 1
        if (gap > 0) fb.skip(gap)
      }
    case None => fb.skip(c.size)
  }
}
