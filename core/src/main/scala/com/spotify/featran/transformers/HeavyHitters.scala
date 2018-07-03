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

import com.spotify.featran.{FeatureBuilder, FlatReader, FlatWriter}
import com.twitter.algebird._

import scala.util.Random

/**
 * Transform a collection of categorical features to 2 columns, one for rank and one for count.
 * Only the top heavyHittersCount items are tracked, with 1.0 being the most frequent rank, 2.0
 * the second most, etc. All other items are transformed to [0.0, 0.0].
 *
 * Ranks and frequencies are estimated with Algebird's SketchMap data structure. With probability
 * at least `1 - delta`, this estimate is within `eps * N` of the true frequency (i.e.,
 * `true frequency <= estimate <= true frequency + eps * N`), where N is the total size of the
 * input collection.
 *
 * Missing values are transformed to [0.0, 0.0].
 */
object HeavyHitters extends SettingsBuilder {

  /**
   * Create a new [[HeavyHitters]] instance.
   * @param heavyHittersCount number of heavy hitters to keep track of
   * @param eps one-sided error bound on the error of each point query, i.e. frequency estimate
   * @param delta a bound on the probability that a query estimate does not lie within some small
   *              interval (an interval that depends on `eps`) around the truth
   * @param seed a seed to initialize the random number generator used to create the pairwise
   *             independent hash functions
   */
  def apply(name: String,
            heavyHittersCount: Int,
            eps: Double = 0.001,
            delta: Double = 0.001,
            seed: Int = Random.nextInt)
    : Transformer[String, SketchMap[String, Long], Map[String, (Int, Long)]] =
    new HeavyHitters(name, heavyHittersCount, eps, delta, seed)

  /**
   * Create a new [[HeavyHitters]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(
    setting: Settings): Transformer[String, SketchMap[String, Long], Map[String, (Int, Long)]] = {
    val seed = setting.params("seed").toInt
    val eps = setting.params("eps").toDouble
    val delta = setting.params("delta").toDouble
    val heavyHittersCount = setting.params("heavyHittersCount").toInt
    HeavyHitters(setting.name, heavyHittersCount, eps, delta, seed)
  }
}

private[featran] class HeavyHitters(name: String,
                                    val heavyHittersCount: Int,
                                    val eps: Double,
                                    val delta: Double,
                                    val seed: Int)
    extends Transformer[String, SketchMap[String, Long], Map[String, (Int, Long)]](name) {

  @transient private lazy val sketchMapParams =
    SketchMapParams[String](seed, eps, delta, heavyHittersCount)(_.getBytes)

  @transient override lazy val aggregator
    : Aggregator[String, SketchMap[String, Long], Map[String, (Int, Long)]] =
    SketchMap
      .aggregator[String, Long](sketchMapParams)
      .composePrepare[String]((_, 1L))
      .andThenPresent { sm =>
        val b = Map.newBuilder[String, (Int, Long)]
        sm.heavyHitterKeys.iterator.zipWithIndex.foreach {
          case (k, r) =>
            b += ((k, (r + 1, sketchMapParams.frequency(k, sm.valuesTable))))
        }
        b.result()
      }
  override def featureDimension(c: Map[String, (Int, Long)]): Int = 2
  override def featureNames(c: Map[String, (Int, Long)]): Seq[String] =
    Seq(s"${name}_rank", s"${name}_freq")
  override def buildFeatures(a: Option[String],
                             c: Map[String, (Int, Long)],
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      c.get(x) match {
        case Some((rank, freq)) =>
          fb.add(s"${name}_rank", rank)
          fb.add(s"${name}_freq", freq)
        case None => fb.skip(2)
      }
    case None => fb.skip(2)
  }
  override def encodeAggregator(c: Map[String, (Int, Long)]): String =
    c.map {
        case (key, (rank, freq)) =>
          s"${URLEncoder.encode(key, "UTF-8")}:$rank:$freq"
      }
      .mkString(",")
  override def decodeAggregator(s: String): Map[String, (Int, Long)] = {
    val kvs = s.split(",")
    val b = Map.newBuilder[String, (Int, Long)]
    kvs.foreach { kv =>
      val t = kv.split(":")
      b += ((URLDecoder.decode(t(0), "UTF-8"), (t(1).toInt, t(2).toLong)))
    }
    b.result()
  }
  override def params: Map[String, String] =
    Map("seed" -> seed.toString,
        "eps" -> eps.toString,
        "delta" -> delta.toString,
        "heavyHittersCount" -> heavyHittersCount.toString)

  def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readString(name)
  def flatWriter[T](implicit fw: FlatWriter[T]): Option[String] => fw.IF =
    fw.writeString(name)
}
