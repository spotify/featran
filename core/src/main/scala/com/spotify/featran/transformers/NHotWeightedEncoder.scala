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
import scala.collection.mutable.{Map => MMap, Set => MSet}

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
 * Missing values are either transformed to zero vectors or encoded as a missing value.
 *
 * When using aggregated feature summary from a previous session, unseen labels are either
 * transformed to zero vectors or encoded as __unknown__ (if encodeMissingValue is true) and
 * [FeatureRejection.Unseen]] rejections are reported.
 */
object NHotWeightedEncoder {
  /**
   * Create a new [[NHotWeightedEncoder]] instance.
   */
  def apply(name: String, encodeMissingValue: Boolean = false)
  : Transformer[Seq[WeightedLabel], Set[String], SortedMap[String, Int]] =
    new NHotWeightedEncoder(name, encodeMissingValue)

  /** Extra definition for java compatibility. */
  def apply(name: String)
  : Transformer[Seq[WeightedLabel], Set[String], SortedMap[String, Int]] =
    new NHotWeightedEncoder(name, false)
}

private class NHotWeightedEncoder(name: String, encodeMissingValue: Boolean = false)
  extends BaseHotEncoder[Seq[WeightedLabel]](name, encodeMissingValue) {
  override def prepare(a: Seq[WeightedLabel]): Set[String] = Set(a.map(_.name): _*)

  /**
   * Transform sequence of weighted labels to a map where the key is the label name and the
   * value is the weight value. If encodeMissingValue is true then check to see if any of
   * the label names are not in the SortedMap. If this is the case then an additional
   * element is added to the list where the key is __unknown__ and the value is the
   * sum over all the weights of the missing labels.
   */
  def getWeights(xs: Seq[WeightedLabel], c: SortedMap[String, Int]): MMap[String, Double] = {
    val weights = MMap.empty[String, Double].withDefaultValue(0.0)
    xs.foreach(x => weights(x.name) += x.value)
    encodeMissingValue match {
      case true => {
        // check if an item is unseen
        val missingKeys = weights.keySet.filter(!c.contains(_)).toSet
        missingKeys.size match {
          case 0 => weights
          case _ =>
            // sum weights of missing items
            val defaultValue = xs.filter(x => missingKeys.contains(x.name)).map(_.value).sum
            weights(missingValueToken) += defaultValue
            weights
        }
      }
      case false => weights
    }
  }

  override def buildFeatures(a: Option[Seq[WeightedLabel]],
                             c: SortedMap[String, Int],
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(xs) =>
      val weights = getWeights(xs, c)
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
    case None => addMissingItem(c, fb)
  }
}
