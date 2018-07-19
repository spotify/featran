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

import breeze.stats.distributions.VonMises
import com.spotify.featran.{FeatureBuilder, FlatReader, FlatWriter}
import com.twitter.algebird.Aggregator

/**
 * Transform a column of continuous features that represent the mean of a von Mises distribution
 * to n columns of continuous features. The number n represent the number of points to evaluate
 * the von Mises distribution. The von Mises pdf is given by
 *
 * f(x | mu, kappa, scale) = exp(kappa * cos(scale*(x-mu)) / (2*pi*Io(kappa))
 *
 * and is only valid for x, mu in the interval [0, 2*pi/scale].
 */
object VonMisesEvaluator extends SettingsBuilder {

  /**
   * Create a new [[VonMisesEvaluator]] instance.
   * @param kappa measure of concentration
   * @param scale scaling factor
   * @param points points to evaluate the distribution with
   */
  def apply(name: String,
            kappa: Double,
            scale: Double,
            points: Array[Double]): Transformer[Double, Unit, Unit] =
    new VonMisesEvaluator(name, kappa, scale, points)

  /**
   * Create a new [[VonMisesEvaluator]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Double, Unit, Unit] = {
    val params = setting.params
    val k = params("kappa").toDouble
    val s = params("scale").toDouble
    val str = params("points")
    val points = str.slice(1, str.length - 1).split(",").map(_.toDouble)
    VonMisesEvaluator(setting.name, k, s, points)
  }

  def getProbability(x: Double, mu: Double, kappa: Double, scale: Double): Double = {
    val muScaled = mu * scale
    val vm = VonMises(muScaled, kappa)
    vm.pdf(scale * x)
  }
}

private[featran] class VonMisesEvaluator(name: String,
                                         val kappa: Double,
                                         val scale: Double,
                                         val points: Array[Double])
    extends Transformer[Double, Unit, Unit](name) {

  private val pMax = points.max
  private val upperBound = 2 * math.Pi / scale
  checkRange("point", pMax, 0.0, upperBound)
  override val aggregator: Aggregator[Double, Unit, Unit] =
    Aggregators.unit[Double]
  override def featureDimension(c: Unit): Int = points.length
  override def featureNames(c: Unit): Seq[String] = names(points.length)

  override def buildFeatures(a: Option[Double], c: Unit, fb: FeatureBuilder[_]): Unit = a match {
    case Some(mu) =>
      checkRange("mu", mu, 0.0, upperBound)
      val probs =
        points.map(VonMisesEvaluator.getProbability(_, mu, kappa, scale))
      fb.add(names(points.length), probs)
    case None => fb.skip(points.length)
  }

  override def encodeAggregator(c: Unit): String = ""
  override def decodeAggregator(s: String): Unit = ()
  override def params: Map[String, String] =
    Map("kappa" -> kappa.toString,
        "scale" -> scale.toString,
        "points" -> points.mkString("[", ",", "]"))

  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readDouble(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[Double] => fw.IF =
    fw.writeDouble(name)
}
