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

import breeze.linalg._
import com.spotify.featran.FeatureBuilder
import com.twitter.algebird.Aggregator

object Normalizer {
  // Missing value = [0.0, 0.0, ...]
  def apply(name: String, p: Double = 2.0): Transformer[Array[Double], Int, Int] =
    new Normalizer(name, p)
}

private class Normalizer(name: String, val p: Double)
  extends Transformer[Array[Double], Int, Int](name) {
  require(p >= 1.0, "p must be >= 1.0")
  override val aggregator: Aggregator[Array[Double], Int, Int] = Aggregators.arrayLength
  override def featureDimension(c: Int): Int = c
  override def featureNames(c: Int): Seq[String] = (0 until c).map(name + "_" + _)
  override def buildFeatures(a: Option[Array[Double]], c: Int,
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      val dv = DenseVector(x)
      fb.add((dv / norm(dv, p)).data)
    case None => fb.skip(c)
  }
  override def encodeAggregator(c: Option[Int]): Option[String] = c.map(_.toString)
  override def decodeAggregator(s: Option[String]): Option[Int] = s.map(_.toInt)
  override def params: Map[String, String] = Map("p" -> p.toString)

}
