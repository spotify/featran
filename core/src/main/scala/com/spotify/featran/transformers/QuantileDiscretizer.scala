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

import scala.collection.JavaConverters._

/**
 * Transform a column of continuous features to n columns of binned categorical features. The
 * number of bins is set by the `numBuckets` parameter.
 *
 * The bin ranges are chosen using the Algebird's QTree approximate data structure. The precision
 * of the approximation can be controlled with the `k` parameter.
 *
 * Missing values are transformed to zero vectors.
 */
object QuantileDiscretizer {
  /**
   * Create a new [[QuantileDiscretizer]] instance.
   * @param numBuckets number of buckets (quantiles, or categories) into which data points are
   *                   grouped, must be greater than or equal to 2
   * @param k precision of the underlying Algebird QTree approximation
   */
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
        val k = l / 2 + u / 2 // (l + u) might overflow
        if (!m.containsKey(k)) { // in case of too few distinct values
          m.put(k, i - 1)
        }
      }
      m.put(qt.upperBound, numBuckets - 1)
      m
    }
  override def featureDimension(c: JTreeMap[Double, Int]): Int = numBuckets
  override def featureNames(c: JTreeMap[Double, Int]): Seq[String] = names(numBuckets).toSeq
  override def buildFeatures(a: Option[Double], c: JTreeMap[Double, Int],
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      val e = c.higherEntry(x)
      val offset = if (e == null) numBuckets - 1 else e.getValue
      fb.skip(offset)
      fb.add(nameAt(offset), 1.0)
      fb.skip(numBuckets - 1 - offset)
    case None => fb.skip(numBuckets)
  }

  override def encodeAggregator(c: Option[JTreeMap[Double, Int]]): Option[String] =
    c.map(_.asScala.map(kv => s"${kv._1}:${kv._2}").mkString(","))
  override def decodeAggregator(s: Option[String]): Option[JTreeMap[Double, Int]] =
    s.map { x =>
      val m = new JTreeMap[Double, Int]()
      x.split(",").foreach { p =>
        val t = p.split(":")
        m.put(t(0).toDouble, t(1).toInt)
      }
      m
    }
  override def params: Map[String, String] =
    Map("numBuckets" -> numBuckets.toString, "k" -> k.toString)
}
