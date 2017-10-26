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
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.{Set => MSet}
/**
 * Weighted label. Also can be thought as a weighted value in a named sparse vector.
 */
case class WeightedLabel(name: String, value: Double)

/**
 * Transform a collection of weighted categorical features to columns of weight sums, with at most
 * N values.
 *
 * Weights of the same labels in a row are summed instead of 1.0 as is the case with the normal
 * [[NHotEncoder]].
 *
 * Missing values are transformed to [0.0, 0.0, ...].
 *
 * When using aggregated feature summary from a previous session, unseen labels are ignored and
 * [[FeatureRejection.Unseen]] rejections are reported.
 */
object NHotWeightedEncoder {
  /**
   * Create a new [[NHotWeightedEncoder]] instance.
   */
  def apply(name: String)
  : Transformer[Seq[WeightedLabel], Set[String], SortedMap[String, Int]] =
    new NHotWeightedEncoder(name)
}

private class NHotWeightedEncoder(name: String) extends BaseHotEncoder[Seq[WeightedLabel]](name) {
  override def prepare(a: Seq[WeightedLabel]): Set[String] = Set(a.map(_.name): _*)
  override def buildFeatures(a: Option[Seq[WeightedLabel]],
                             c: SortedMap[String, Int],
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(xs) =>
      val weights = MMap.empty[String, Double].withDefaultValue(0.0)
      xs.foreach(x => weights(x.name) += x.value)

      val keys = weights.keySet.toList.sorted
      var prev = -1
      var totalSeen = MSet[String]()
      keys.foreach { key =>
        c.get(key) match {
          case Some(curr) =>
            val gap = curr - prev - 1
            if (gap > 0) fb.skip(gap)
            fb.add(name + '_' + key, weights(key))
            prev = curr
            totalSeen += key
          case None =>
        }
      }
      val gap = c.size - prev - 1
      if (gap > 0) fb.skip(gap)

      if (totalSeen.size != keys.size) {
        val unseen = keys.toSet -- totalSeen
        fb.reject(this, FeatureRejection.Unseen(unseen))
      }
    case None => fb.skip(c.size)
  }
}
