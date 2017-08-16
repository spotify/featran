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

import com.spotify.featran.{FeatureBuilder, FeatureRejection}
import com.twitter.algebird.Aggregator

import scala.collection.SortedMap

/**
 * Transform a collection of categorical features to binary columns, with at most a single
 * one-value.
 *
 * Missing values are transformed to [0.0, 0.0, ...].
 *
 * When using aggregated feature summary from a previous session, unseen labels are ignored and
 * [[FeatureRejection.Unseen]] rejections are reported.
 */
object OneHotEncoder {
  /**
   * Create a new [[OneHotEncoder]] instance.
   */
  def apply(name: String): Transformer[String, Set[String], SortedMap[String, Int]] =
    new OneHotEncoder(name)
}

private class OneHotEncoder(name: String) extends BaseHotEncoder[String](name) {
  override def prepare(a: String): Set[String] = Set(a)
  override def buildFeatures(a: Option[String],
                             c: SortedMap[String, Int],
                             fb: FeatureBuilder[_]): Unit = {
    a match {
      case Some(k) => c.get(k) match {
        case Some(v) =>
          fb.skip(v)
          fb.add(name + '_' + k, 1.0)
          fb.skip(math.max(0, c.size - v - 1))
        case None =>
          fb.skip(c.size)
          fb.reject(this, FeatureRejection.Unseen(Set(k)))
      }
      case None => fb.skip(c.size)
    }
  }
}

private abstract class BaseHotEncoder[A](name: String)
  extends Transformer[A, Set[String], SortedMap[String, Int]](name) {

  def prepare(a: A): Set[String]

  private def present(reduction: Set[String]): SortedMap[String, Int] = {
    val b = SortedMap.newBuilder[String, Int]
    var i = 0
    val array = reduction.toArray
    java.util.Arrays.sort(array, Ordering[String])
    while (i < array.length) {
      b += array(i) -> i
      i += 1
    }
    b.result()
  }
  override val aggregator: Aggregator[A, Set[String], SortedMap[String, Int]] =
    Aggregators.from[A](prepare).to(present)
  override def featureDimension(c: SortedMap[String, Int]): Int = c.size
  override def featureNames(c: SortedMap[String, Int]): Seq[String] = {
    c.map(name + '_' + _._1)(scala.collection.breakOut)
  }

  override def encodeAggregator(c: Option[SortedMap[String, Int]]): Option[String] =
    c.map(_.map(e => "label:" + URLEncoder.encode(e._1, "UTF-8")).mkString(","))
  override def decodeAggregator(s: Option[String]): Option[SortedMap[String, Int]] = s.map { ks =>
    val a = ks.split(",").filter(_.nonEmpty)
    var i = 0
    val b = SortedMap.newBuilder[String, Int]
    while (i < a.length) {
      b += URLDecoder.decode(a(i).replaceAll("^label:", ""), "UTF-8") -> i
      i += 1
    }
    b.result()
  }

}
