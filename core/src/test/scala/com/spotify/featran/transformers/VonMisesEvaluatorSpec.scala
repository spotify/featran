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
import org.scalacheck._

object VonMisesEvaluatorSpec extends TransformerProp("VonMisesEvaluator") {

  private val MinPoint = 0.0
  private val MaxPoint = 1000.0
  private val Scale = 2 * math.Pi / MaxPoint

  private val MuGen = Gen.nonEmptyListOf(Gen.choose(MinPoint, MaxPoint))
  private val PointGen =
    Gen.choose(3, 10).flatMap(n => Gen.listOfN(n, Gen.choose(MinPoint, MaxPoint)))
  private val KappaGen = Gen.choose(0.0, 100.0)

  property("default") = Prop.forAll(MuGen, PointGen, KappaGen) { (xs, points, kappa) =>
    val dim = points.size
    val names = (0 until dim).map("vm_" + _)
    val missing = (0 until dim).map(_ => 0.0)
    val expected = xs.map { mu =>
      points.map(p => VonMises(mu * Scale, kappa).pdf(Scale * p))
    }
    test(VonMisesEvaluator("vm", kappa, Scale, points.toArray), xs, names, expected, missing)
  }

}
