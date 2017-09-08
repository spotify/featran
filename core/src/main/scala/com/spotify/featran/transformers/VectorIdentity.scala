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

import com.spotify.featran.{FeatureBuilder, FeatureRejection}
import com.twitter.algebird.Aggregator

import scala.language.higherKinds

/**
 * Takes fixed length vectors by passing them through.
 *
 * Similar to [[Identity]] but for a sequence of doubles.
 *
 * Missing values are transformed to zero vectors.
 *
 * When using aggregated feature summary from a previous session, vectors of different dimensions
 * are transformed to zero vectors and [[FeatureRejection.WrongDimension]] rejections are reported.
 */
object VectorIdentity {
  /**
   * Create a new [[VectorIdentity]] instance.
   * @param expectedLength expected length of the input vectors, or 0 to infer from data
   */
  def apply[M[_]](name: String, expectedLength: Int = 0)
                 (implicit ev: M[Double] => Seq[Double]): Transformer[M[Double], Int, Int] =
    new VectorIdentity(name, expectedLength)(ev)
}

private class VectorIdentity[M[_]](name: String, expectedLength: Int)
                                  (implicit ev: M[Double] => Seq[Double])
  extends Transformer[M[Double], Int, Int](name) {
  override val aggregator: Aggregator[M[Double], Int, Int] = Aggregators.seqLength(expectedLength)
  override def featureDimension(c: Int): Int = c
  override def featureNames(c: Int): Seq[String] = names(c).toSeq
  override def buildFeatures(a: Option[M[Double]], c: Int, fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      if (x.length != c) {
        fb.skip(c)
        fb.reject(this, FeatureRejection.WrongDimension(c, x.length))
      } else {
        fb.add(names(c), x)
      }
    case None => fb.skip(c)
  }

  override def encodeAggregator(c: Option[Int]): Option[String] = c.map(_.toString)
  override def decodeAggregator(s: Option[String]): Option[Int] = s.map(_.toInt)
  override def params: Map[String, String] = Map("expectedLength" -> expectedLength.toString)
}
