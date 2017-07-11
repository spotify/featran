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
import com.twitter.algebird.Aggregator

object VectorIdentity {
  /**
   * Takes a vector with a fixed length and maps it to the features.
   *
   * Similar to Identity but for a Sequence of Doubles.
   */
  def apply(name: String, length: Int): Transformer[Seq[Double], Unit, Unit] =
    new VectorIdentity(name, length)
}

private class VectorIdentity(name: String, length: Int)
  extends Transformer[Seq[Double], Unit, Unit](name) {
  require(length > 0, "length must be > 0")
  private val names = 0.until(length).map(name + "_" + _)

  override val aggregator: Aggregator[Seq[Double], Unit, Unit] = Aggregators.unit
  override def featureDimension(c: Unit): Int = length
  override def featureNames(c: Unit): Seq[String] = names

  override def buildFeatures(a: Option[Seq[Double]], c: Unit, fb: FeatureBuilder[_]): Unit =
    if(a.isDefined) fb.add(names.toIterator, a.get.toArray) else fb.skip(length)

  override def encodeAggregator(c: Option[Unit]): Option[String] = c.map(_ => "")
  override def decodeAggregator(s: Option[String]): Option[Unit] = s.map(_ => ())
  override def params: Map[String, String] = Map("length" -> length.toString)
}
