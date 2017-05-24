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

import java.util.{TreeMap => JTreeMap}

import com.spotify.featran.FeatureBuilder
import com.twitter.algebird.Aggregator

object Bucketizer {
  // Missing value = [0.0, 0.0, ...]
  def apply(name: String, splits: Array[Double]): Transformer[Double, Unit, Unit] =
    new Bucketizer(name, splits)
}

private class Bucketizer(name: String, splits: Array[Double])
  extends Transformer[Double, Unit, Unit](name) {
  require(splits.length >= 3, "splits.length must be >= 3")
  private val lower = splits.head
  private val map = {
    val m = new JTreeMap[Double, Int]()
    var i = 1
    while (i < splits.length) {
      require(splits(i) > splits(i - 1), "splits must be in increasing order")
      m.put(splits(i), i - 1)
      i += 1
    }
    m
  }
  override val aggregator: Aggregator[Double, Unit, Unit] = Aggregators.unit[Double]
  override def featureDimension(c: Unit): Int = splits.length - 1
  override def featureNames(c: Unit): Seq[String] = nameN(splits.length - 1)
  override def buildFeatures(a: Option[Double], c: Unit, fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      val e = map.higherEntry(x)
      if (x < lower || e == null) {
        (0 until splits.length - 1).foreach(_ => fb.skip())
      } else {
        val offset = e.getValue
        (0 until splits.length - 1).foreach(i => if (i == offset) fb.add(1.0) else fb.skip())
      }
    case None => fb.skip(splits.length - 1)
  }

  override def encodeAggregator(c: Option[Unit]): Option[String] = c.map(_ => "")
  override def decodeAggregator(s: Option[String]): Option[Unit] = s.map(_ => ())
  override def params: Map[String, String] = Map("splits" -> splits.mkString("[", ",", "]"))
}
