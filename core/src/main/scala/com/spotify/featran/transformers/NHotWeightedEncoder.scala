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

import scala.collection.mutable.{Map => MMap}

/**
 * Weighted value. Also can be thought as a weighted label or a value in a named sparse vector.
 */
case class WeightedValue(name: String, value: Double)

object NHotWeightedEncoder {
  /**
   * Transform a collection of weighted categorical features to columns of weight sums, with at
   * most N values.
   *
   * Weights of the same labels in a row are summed instead of 1.0 as is the case with the normal
   * [[NHotEncoder]].
   *
   * Missing values are transformed to [0.0, 0.0, ...].
   *
   * When using aggregated feature summary from a previous session, unseen labels are ignored.
   */
  def apply(name: String): Transformer[Seq[WeightedValue], Set[String], Array[String]] =
    new NHotWeightedEncoder(name)
}

private class NHotWeightedEncoder(name: String)
  extends Transformer[Seq[WeightedValue], Set[String], Array[String]](name) {
  override val aggregator: Aggregator[Seq[WeightedValue], Set[String], Array[String]] =
    Aggregators.from[Seq[WeightedValue]](_.map(_.name).toSet).to(_.toArray.sorted)
  override def featureDimension(c: Array[String]): Int = c.length
  override def featureNames(c: Array[String]): Seq[String] = c.map(name + "_" + _).toSeq
  override def buildFeatures(a: Option[Seq[WeightedValue]], c: Array[String],
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(xs) =>
      val as = MMap[String, Double]().withDefaultValue(0.0)
      xs.foreach(x => as(x.name) += x.value)
      c.foreach(s => if (as.contains(s)) fb.add(name + "_" + s, as(s)) else fb.skip())
    case None => fb.skip(c.length)
  }

  override def encodeAggregator(c: Option[Array[String]]): Option[String] =
    c.map(_.map(URLEncoder.encode(_, "UTF-8")).mkString(","))
  override def decodeAggregator(s: Option[String]): Option[Array[String]] =
    s.map(_.split(",").map(URLDecoder.decode(_, "UTF-8")))
}
