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
import com.spotify.featran.FeatureBuilder
import com.twitter.algebird.Aggregator


object VonMisesEvaluator {
  /**
    * Transform a column of continuous features that represent the mean of a von Mises distribution
    * to n columns of continuous features. The number n represent the number of points to
    * evaluate the von Mises distribution. The von Mises pdf is given by
    *
    * f(x | mu, kappa, scale) = exp(kappa * cos(scale*(x-mu)) / (2*pi*Io(kappa))
    *
    * and is only valid for x, mu in the interval [0, 2*pi/scale].
    */
  def apply(name: String, kappa: Double, scale: Double, points: Array[Double]):
  Transformer[Double, Unit, Unit] =
    new VonMisesEvaluator(name, kappa, scale, points)

  def getProbability(x: Double, mu: Double, kappa: Double, scale: Double): Double = {
    val muScaled = mu * scale
    val vm = VonMises(muScaled, kappa)
    vm.pdf(scale*x)
  }
}

private class VonMisesEvaluator(name: String,
                                kappa: Double,
                                scale: Double,
                                points: Array[Double])
  extends Transformer[Double, Unit, Unit](name) {

  private val pMax = points.max
  private val upperBound = 2*math.Pi/scale
  require(pMax >= 0 && pMax <= upperBound,
    "point=%.3f not in the range [0, %.3f] (given by [0,2*pi/scale])".format(
      pMax, upperBound)
  )
  override val aggregator: Aggregator[Double, Unit, Unit] = Aggregators.unit[Double]
  override def featureDimension(c: Unit): Int = points.length
  override def featureNames(c: Unit): Seq[String] = names(points.length).toSeq

  override def buildFeatures(a: Option[Double], c: Unit, fb: FeatureBuilder[_]): Unit = a match {
    case Some(mu) => {
      require(mu >= 0 && mu <= upperBound,
        "mu=%.3f not in the range [0, %.3f] (given by [0,2*pi/scale])".format(
          mu, upperBound)
      )
      points.zipWithIndex.foreach { case (p: Double, i: Int) =>
        val v = VonMisesEvaluator.getProbability(p, mu, kappa, scale)
        fb.add(nameAt(i), v)
      }
    }
    case None => fb.skip(points.length)
  }

  override def encodeAggregator(c: Option[Unit]): Option[String] = c.map(_ => "")
  override def decodeAggregator(s: Option[String]): Option[Unit] = s.map(_ => ())
  override def params: Map[String, String] =
    Map("points" -> points.mkString("[", ",", "]"),
      "kappa" -> kappa.toString,
      "scale" -> scale.toString)
}
