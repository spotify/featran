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

import java.net.{URLDecoder, URLEncoder}

import com.spotify.featran.FeatureBuilder
import com.twitter.algebird.Aggregator

import scala.collection.mutable.HashMap

object OneHotEncoder {
  /**
   * Transform a collection of categorical features to binary columns, with at most a single
   * one-value.
   *
   * Missing values are transformed to [0.0, 0.0, ...].
   *
   * When using aggregated feature summary from a previous session, unseen labels are ignored.
   */
  def apply(name: String): Transformer[String, Set[String], HashMap[String, Int]] =
    new OneHotEncoder(name)
}

private class OneHotEncoder(name: String)
  extends Transformer[String, Set[String], HashMap[String, Int]](name) {

  private def present(reduction: Set[String]): HashMap[String, Int] = {
    val map = new HashMap[String, Int]

    // if the set size is under 1000 first sort elements (also needed for unit tests)
    val maxSizeForSorting = 1000
    if (reduction.size <= maxSizeForSorting) {
      reduction.toArray.sorted.zipWithIndex.foreach { case (k: String, i: Int) => map += k -> i }
    } else {
      reduction.zipWithIndex.foreach { case (k: String, i: Int) => map += k -> i }
    }
    map
  }

  override val aggregator: Aggregator[String, Set[String], HashMap[String, Int]] =
    Aggregators.from[String](Set(_)).to(s => present(s))
  override def featureDimension(c: HashMap[String, Int]): Int = c.size
  override def featureNames(c: HashMap[String, Int]): Seq[String] =
    c.toList.sortBy(_._2).map(name + '_' + _._1)
  override def buildFeatures(a: Option[String],
                             c: HashMap[String, Int],
                             fb: FeatureBuilder[_]): Unit = {
    val n = c.size
    a match {
      case Some(key) => {
        c.get(key) match {
          case Some(value) => {
            fb.skip(value)
            fb.add(name + '_' + key, 1.0)
            fb.skip(math.max(0, n-value-1))
          }
          case None => fb.skip(n)
        }
      }
      case None => fb.skip(n)
    }
  }
  override def encodeAggregator(c: Option[HashMap[String, Int]]): Option[String] =
    c.map(_.toList.sortBy(_._2).map(e => URLEncoder.encode(e._1, "UTF-8")).mkString(","))
  override def decodeAggregator(s: Option[String]): Option[HashMap[String, Int]] = {
    s match {
      case Some(s) => {
        val keys = s.split(",").map(URLDecoder.decode(_, "UTF-8"))
        val map = new HashMap[String, Int]
        keys.zipWithIndex.foreach { case (k, i) => map += k -> i }
        Some(map)
      }
      case None => None
    }
  }
}
