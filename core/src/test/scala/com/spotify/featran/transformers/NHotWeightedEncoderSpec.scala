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

object NHotWeightedEncoderSpec extends TransformerProp("NHotWeightedEncoder") {

  private implicit val weightedVectors = Arbitrary {
    val weightedValueGen = for {
      value <- Gen.chooseNum(-1.0, 1.0)
      n <- Gen.alphaStr
    } yield WeightedLabel(n, value)

    Gen.choose(1, 5).flatMap(Gen.listOfN(_, weightedValueGen))
  }

  property("default") = Prop.forAll { xs: List[List[WeightedLabel]] =>
    val cats = xs.flatten.map(_.name).distinct.sorted
    val names = cats.map("n_hot_" + _)
    val expected =
      xs.map(s => cats.map(c => s.filter(_.name == c).map(_.value).sum))
    val missing = cats.map(_ => 0.0)
    val oob =
      List((List(WeightedLabel("s1", 0.2), WeightedLabel("s2", 0.1)), missing))
    test(NHotWeightedEncoder("n_hot"), xs, names, expected, missing, oob)
  }

  property("encodeMissingValue") = Prop.forAll { xs: List[List[WeightedLabel]] =>
    import MissingValue.MissingValueToken
    val cats = xs.flatten.map(_.name).distinct.sorted :+ MissingValueToken
    val names = cats.map("n_hot_" + _)
    val expected =
      xs.map(s => cats.map(c => s.filter(_.name == c).map(_.value).sum))
    val missingBase = cats.map(c => if (c == MissingValueToken) 1.0 else 0.0)

    val oob = List(
      (List(WeightedLabel("s1", 0.2), WeightedLabel("s2", 0.1)), missingBase.map(v => v * 0.3)))
    test(NHotWeightedEncoder("n_hot", encodeMissingValue = true),
         xs,
         names,
         expected,
         missingBase,
         oob)
  }

}
