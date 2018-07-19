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

import com.spotify.featran.{FeatureBuilder, FeatureRejection, FlatReader, FlatWriter}
import com.twitter.algebird._

import scala.collection.JavaConverters._

/**
 * Transform a column of continuous features to n columns of binned categorical features. The
 * number of bins is set by the `numBuckets` parameter.
 *
 * The bin ranges are chosen using the Algebird's QTree approximate data structure. The precision
 * of the approximation can be controlled with the `k` parameter.
 *
 * Missing values are transformed to zero vectors.
 *
 * When using aggregated feature summary from a previous session, values outside of previously seen
 * `[min, max]` are binned into the first or last bucket and [[FeatureRejection.OutOfBound]]
 * rejections are reported.
 */
object QuantileDiscretizer extends SettingsBuilder {

  /**
   * Create a new [[QuantileDiscretizer]] instance.
   * @param numBuckets number of buckets (quantiles, or categories) into which data points are
   *                   grouped, must be greater than or equal to 2
   * @param k precision of the underlying Algebird QTree approximation
   */
  def apply(name: String,
            numBuckets: Int = 2,
            k: Int = QTreeAggregator.DefaultK): Transformer[Double, B, C] =
    new QuantileDiscretizer(name, numBuckets, k)

  /**
   * Create a new [[QuantileDiscretizer]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Double, B, C] = {
    val numBuckets = setting.params("numBuckets").toInt
    val k = setting.params("k").toInt
    QuantileDiscretizer(setting.name, numBuckets, k)
  }

  private type B = (QTree[Double], Min[Double], Max[Double])
  private type C = (JTreeMap[Double, Int], Double, Double)
}

private[featran] class QuantileDiscretizer(name: String, val numBuckets: Int, val k: Int)
    extends Transformer[Double, QuantileDiscretizer.B, QuantileDiscretizer.C](name) {
  require(numBuckets >= 2, "numBuckets must be >= 2")

  import QuantileDiscretizer.{B, C}
  implicit val sg = new QTreeSemigroup[Double](k)

  override val aggregator: Aggregator[Double, B, C] =
    Aggregators.from[Double](x => (QTree(x), Min(x), Max(x))).to {
      case (qt, min, max) =>
        val m = new JTreeMap[Double, Int]() // upper bound -> offset
        val interval = 1.0 / numBuckets
        for (i <- 1 until numBuckets) {
          val (l, u) = qt.quantileBounds(interval * i)
          val k = l / 2 + u / 2 // (l + u) might overflow
          if (!m.containsKey(k)) { // in case of too few distinct values
            m.put(k, i - 1)
          }
        }
        m.put(qt.upperBound, numBuckets - 1)
        (m, min.get, max.get)
    }
  override def featureDimension(c: C): Int = numBuckets
  override def featureNames(c: C): Seq[String] = names(numBuckets)
  override def buildFeatures(a: Option[Double], c: C, fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      val (m, min, max) = c
      val e = m.higherEntry(x)
      val offset = if (e == null) numBuckets - 1 else e.getValue
      fb.skip(offset)
      fb.add(nameAt(offset), 1.0)
      fb.skip(numBuckets - 1 - offset)
      if (x < min || x > max) {
        fb.reject(this, FeatureRejection.OutOfBound(min, max, x))
      }
    case None => fb.skip(numBuckets)
  }

  override def encodeAggregator(c: C): String = {
    val (m, min, max) = c
    s"$min,$max," + m.asScala.map(kv => s"${kv._1}:${kv._2}").mkString(",")
  }
  override def decodeAggregator(s: String): C = {
    val a = s.split(",")
    val (min, max) = (a(0).toDouble, a(1).toDouble)
    val m = new JTreeMap[Double, Int]()
    (2 until a.length).foreach { i =>
      val t = a(i).split(":")
      m.put(t(0).toDouble, t(1).toInt)
    }
    (m, min, max)
  }
  override def params: Map[String, String] =
    Map("numBuckets" -> numBuckets.toString, "k" -> k.toString)

  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readDouble(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[Double] => fw.IF =
    fw.writeDouble(name)
}
