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

import org.scalacheck.{Arbitrary, Gen, Prop}

class NHotWeightedEncoderSpec extends TransformerProp("NHotWeightedEncoder") {

  private implicit val weightedVectors = Arbitrary {
    val weightedValueGen = for {
      value <- Gen.chooseNum(-1.0, 1.0)
      n <- Gen.alphaStr
    } yield WeightedValue(n, value)

    Gen.choose(1, 5).flatMap(Gen.listOfN(_, weightedValueGen))
  }

  property("default") = Prop.forAll { xs: List[List[WeightedValue]] =>
    val cats = xs.flatten.map(_.name).distinct.sorted
    val names = cats.map("n_hot_" + _)
    val expected = xs.map(s => cats.map(c => (0.0 +: s.filter(_.name == c).map(_.value)).sum))
    val missing = cats.map(_ => 0.0)
    val oob = List((List(WeightedValue("s1", 0.2), WeightedValue("s2", 0.1)), missing))
    test[Seq[WeightedValue]](NHotWeightedEncoder("n_hot"), xs, names, expected, missing, oob)
  }

}
