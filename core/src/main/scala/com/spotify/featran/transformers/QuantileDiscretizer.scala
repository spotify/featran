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
import com.twitter.algebird.{Aggregator, QTree, QTreeAggregator, QTreeSemigroup}

object QuantileDiscretizer {
  // Missing value = [0.0, 0.0, ...]
  def apply(name: String, numBuckets: Int = 2, k: Int = QTreeAggregator.DefaultK)
  : Transformer[Double, QTree[Double], JTreeMap[Double, Int]] =
    new QuantileDiscretizer(name, numBuckets, k)
}

private class QuantileDiscretizer(name: String, val numBuckets: Int, val k: Int)
  extends Transformer[Double, QTree[Double], JTreeMap[Double, Int]](name) {
  require(numBuckets >= 2, "numBuckets must be >= 2")
  implicit val sg = new QTreeSemigroup[Double](k)
  override val aggregator: Aggregator[Double, QTree[Double], JTreeMap[Double, Int]] =
    Aggregators.from[Double](QTree(_)).to { qt =>
      val m = new JTreeMap[Double, Int]()  // upper bound -> offset
      val interval = 1.0 / numBuckets
      for (i <- 1 until numBuckets) {
        val (l, u) = qt.quantileBounds(interval * i)
        m.put(l + (u - l) / 2, i - 1)
      }
      m.put(qt.upperBound, numBuckets - 1)
      m
    }
  override def featureDimension(c: JTreeMap[Double, Int]): Int = numBuckets
  override def featureNames(c: JTreeMap[Double, Int]): Seq[String] =
    (0 until numBuckets).map(name + "_" + _)
  override def buildFeatures(a: Option[Double], c: JTreeMap[Double, Int],
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      val offset = c.higherEntry(x).getValue
      (0 until numBuckets).foreach(i => if(i == offset) fb.add(1.0) else fb.skip())
    case None => (0 until numBuckets).foreach(_ => fb.skip())
  }
}
