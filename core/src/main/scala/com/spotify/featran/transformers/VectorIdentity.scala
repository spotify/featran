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

import com.spotify.featran.{FeatureBuilder, FeatureRejection, FlatReader, FlatWriter}
import com.twitter.algebird.Aggregator

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
object VectorIdentity extends SettingsBuilder {

  /**
   * Create a new [[VectorIdentity]] instance.
   * @param expectedLength expected length of the input vectors, or 0 to infer from data
   */
  def apply[M[_]](name: String, expectedLength: Int = 0)(
    implicit ev: M[Double] => Seq[Double]): Transformer[M[Double], Int, Int] =
    new VectorIdentity(name, expectedLength)(ev)

  /**
   * Create a new [[VectorIdentity]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Seq[Double], Int, Int] = {
    val el = setting.params("expectedLength").toInt
    VectorIdentity[Seq](setting.name, el)
  }
}

private[featran] class VectorIdentity[M[_]](name: String, val expectedLength: Int)(
  implicit ev: M[Double] => Seq[Double])
    extends Transformer[M[Double], Int, Int](name) {
  override val aggregator: Aggregator[M[Double], Int, Int] =
    Aggregators.seqLength(expectedLength)
  override def featureDimension(c: Int): Int = c
  override def featureNames(c: Int): Seq[String] = names(c)
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

  override def encodeAggregator(c: Int): String = c.toString
  override def decodeAggregator(s: String): Int = s.toInt
  override def params: Map[String, String] =
    Map("expectedLength" -> expectedLength.toString)

  def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readDoubles(name)
  def flatWriter[T](implicit fw: FlatWriter[T]): Option[M[Double]] => fw.IF =
    (v: Option[M[Double]]) => fw.writeDoubles(name)(v.map(_.toSeq))
}
