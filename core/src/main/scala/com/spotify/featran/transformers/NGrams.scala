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
 * Transform a collection of sentences, where each row is a `Seq[String]` of the words / tokens,
 * into a collection containing all the n-grams that can be constructed from each row. The feature
 * representation is an n-hot encoding (see [[NHotEncoder]]) constructed from an expanded vocabulary
 * of all of the generated n-grams.
 *
 * N-grams are generated based on a specified range of `low` to `high` (inclusive) and are joined by
 * the given `separator` (the default is " "). For example, an [[NGrams]] with
 * `separator = ""`, `low = 2`, and `high = 3`, the row `["a", "b", "c", "d", "e"]` would produce
 * the ngrams `["ab", "bc", "cd", "de", "abc", "bcd", "cde"]`.
 *
 * As with [[NHotEncoder]], missing values are transformed to [0.0, 0.0, ...].
 */
object NGrams {
  /**
   * Create a new [[NGrams]] instance.
   *
   * @param low the smallest size of the generated *-grams. Default is 1.
   * @param high the largest size of the generated *-grams. Default is -1, which means n-grams will
   *             be generated up to the full length of the `Seq[String]`
   * @param sep a string separator used to join individual tokens. Default is " ".
   */
  def apply(name: String, low: Int = 1, high: Int = -1, sep: String = " ")
  : Transformer[Seq[String], Set[String], SortedMap[String, Int]] = {
    require(low > 0, "'low' must be a positive integer")
    require(high > 0 || high == -1, "'high' must either be a positive integer, or -1")
    new NGrams(name, low, high, sep)
  }
}

private class NGrams(name: String, val low: Int, val high: Int, val sep: String)
  extends NHotEncoder(name) {
  override def prepare(a: Seq[String]): Set[String] = ngrams(a).toSet

  override def buildFeatures(a: Option[Seq[String]],
                             c: SortedMap[String, Int],
                             fb: FeatureBuilder[_]): Unit =
    super.buildFeatures(a.map(ngrams(_)), c, fb)

  private[transformers] def ngrams(a: Seq[String]): Seq[String] = {
    val max = if (high == -1) a.length else high
    val xs = a.toStream
    val ngrams = for (i <- low to max) yield xs.sliding(i).map(_.mkString(sep))
    ngrams.flatten
  }
}
