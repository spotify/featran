/*
 * Copyright 2018 Spotify AB.
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
import com.twitter.algebird.{Aggregator, SketchMap, SketchMapParams}

import scala.collection.SortedMap
import scala.util.Random

/**
  * Transform a collection of categorical features to binary columns, with at most a single
  * one-value. Only the top N items are tracked.
  *
  * The list of top N is estimated with Algebird's SketchMap data structure. With probability
  * at least `1 - delta`, this estimate is within `eps * N` of the true frequency (i.e.,
  * `true frequency <= estimate <= true frequency + eps * N`), where N is the total size of the
  * input collection.
  *
  * Missing values are either transformed to zero vectors or encoded as a missing value.
  */
object TopNOneHotEncoder {
/**
  * Create a new [[TopNOneHotEncoder]] instance.
  *
  * @param n               number of items to keep track of
  * @param eps             one-sided error bound on the error of each point query, i.e.
  *                        frequency estimate
  * @param delta           a bound on the probability that a query estimate does not lie within
  *                        some small interval (an interval that depends on `eps`) around the
  *                        truth.
  * @param seed            a seed to initialize the random number generator used to create
  *                        the pairwise independent hash functions.
  * @param missingValueOpt optional name to encode items outside of the top n set.
  */
  def apply(name: String, n: Int,
            eps: Double = 0.001,
            delta: Double = 0.001,
            seed: Int = Random.nextInt,
            missingValueOpt: Option[String] = None)
  : Transformer[String, SketchMap[String, Long], SortedMap[String, Int]] =
    new TopNOneHotEncoder(name, n, eps, delta, seed, missingValueOpt)

  def apply(name: String, n: Int,
            eps: Double,
            delta: Double,
            seed: Int,
            missingValue: String)
  : Transformer[String, SketchMap[String, Long], SortedMap[String, Int]] =
    new TopNOneHotEncoder(name, n, eps, delta, seed, Some(missingValue))

  // extra apply for java compatibility
  def apply(name: String, n: Int,
            eps: Double,
            delta: Double,
            seed: Int)
  : Transformer[String, SketchMap[String, Long], SortedMap[String, Int]] =
    new TopNOneHotEncoder(name, n, eps, delta, seed, None)
}

private class TopNOneHotEncoder(name: String,
                                val n: Int,
                                val eps: Double,
                                val delta: Double,
                                val seed: Int,
                                val missingValueOpt: Option[String])
  extends Transformer[String, SketchMap[String, Long], SortedMap[String, Int]](name) {

  private val sketchMapParams =
    SketchMapParams[String](seed, eps, delta, n)(_.getBytes)

  override val aggregator: Aggregator[String, SketchMap[String, Long], SortedMap[String, Int]] =
    SketchMap
      .aggregator[String, Long](sketchMapParams)
      .composePrepare[String]((_, 1L))
      .andThenPresent { sm =>
        val b = SortedMap.newBuilder[String, Int]
        val topItems = missingValueOpt match {
          case Some(missingValueToken) => sm.heavyHitterKeys :+ missingValueToken
          case _ => sm.heavyHitterKeys
        }
        topItems.sorted.iterator.zipWithIndex.foreach { case (k, r) =>
          b += k -> r
        }
        b.result()
      }

  override def featureDimension(c: SortedMap[String, Int]): Int = c.size

  override def featureNames(c: SortedMap[String, Int]): Seq[String] =
    c.map(name + '_' + _._1)(scala.collection.breakOut)

  def addNonTopItem(c: SortedMap[String, Int],
                    fb: FeatureBuilder[_]): Unit = missingValueOpt match {
    case Some(missingValueToken) =>
      val v = c.get(missingValueToken).get // manually added so will exist
      fb.skip(v)
      fb.add(name + '_' + missingValueToken, 1.0)
      fb.skip(math.max(0, c.size - v - 1))
    case _ => fb.skip(c.size)
  }

  override def buildFeatures(a: Option[String],
                             c: SortedMap[String, Int],
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(k) => c.get(k) match {
      case Some(v) =>
        fb.skip(v)
        fb.add(name + '_' + k, 1.0)
        fb.skip(math.max(0, c.size - v - 1))
      case None =>
        addNonTopItem(c, fb)
        fb.reject(this, FeatureRejection.Unseen(Set(k)))
    }
    case None => addNonTopItem(c, fb)
  }

  override def encodeAggregator(c: SortedMap[String, Int]): String =
    c.map(e => "label:" + URLEncoder.encode(e._1, "UTF-8")).mkString(",")

  override def decodeAggregator(s: String): SortedMap[String, Int] = {
    val a = s.split(",").filter(_.nonEmpty)
    var i = 0
    val b = SortedMap.newBuilder[String, Int]
    while (i < a.length) {
      b += URLDecoder.decode(a(i).replaceAll("^label:", ""), "UTF-8") -> i
      i += 1
    }
    b.result()
  }

  override def params: Map[String, String] = Map(
    "n" -> n.toString,
    "eps" -> eps.toString,
    "delta" -> delta.toString,
    "seed" -> seed.toString,
    "missingValueOpt" -> missingValueOpt.toString)

}
