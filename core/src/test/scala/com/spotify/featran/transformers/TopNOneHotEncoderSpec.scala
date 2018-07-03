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

import com.twitter.algebird._
import org.scalacheck._

object TopNOneHotEncoderSpec extends TransformerProp("TopNOneHotEncoder") {

  private implicit val labelArb: Arbitrary[String] = Arbitrary {
    val infrequent = Gen.listOfN(50, Gen.alphaStr).flatMap(xs => Gen.oneOf(xs))
    val frequent = Gen.listOfN(5, Gen.alphaStr).flatMap(xs => Gen.oneOf(xs))
    Gen.frequency((1, infrequent), (50, frequent))
  }
  private val Seed = 1

  import MissingValue.MissingValueToken

  def getExpectedOutputVector(s: String,
                              cats: List[String],
                              encodeMissingValue: Boolean): Seq[Double] = {
    val v = cats.map(c => if (s == c) 1.0 else 0.0)
    if (encodeMissingValue && v.sum == 0.0) {
      cats.map(c => if (c == MissingValueToken) 1.0 else 0.0)
    } else {
      v
    }
  }

  private def test(transformer: Transformer[String, _, _], xs: List[String]): Prop = {
    val encoder = transformer.asInstanceOf[TopNOneHotEncoder]
    val (n, eps, delta) = (encoder.n, encoder.eps, encoder.delta)
    val encodeMissingValue = encoder.encodeMissingValue

    val params = SketchMapParams[String](Seed, eps, delta, n)(_.getBytes)
    val aggregator = SketchMap.aggregator[String, Long](params)
    val sm =
      xs.map(x => aggregator.prepare((x, 1L))).reduce(aggregator.monoid.plus)
    val keys = sm.heavyHitterKeys.sorted
    val cats = if (encodeMissingValue) keys :+ MissingValueToken else keys
    val names = cats.map("tn1h_" + _)
    val expected =
      xs.map(s => getExpectedOutputVector(s, cats, encodeMissingValue))
    val missing = if (encodeMissingValue) {
      cats.map(c => if (c == MissingValueToken) 1.0 else 0.0)
    } else {
      cats.map(_ => 0.0)
    }
    val oob = List(("s1", missing), ("s2", missing)) // unseen labels
    val rejected =
      xs.flatMap(x => if (cats.contains(x)) None else Some(missing))

    test(transformer, xs, names, expected, missing, oob, rejected)
  }

  property("default") = Prop.forAll { xs: List[String] =>
    test(TopNOneHotEncoder("tn1h", 10, seed = 1), xs)
  }

  property("count") = Prop.forAll { xs: List[String] =>
    test(TopNOneHotEncoder("tn1h", 100, seed = 1), xs)
  }

  property("eps") = Prop.forAll { xs: List[String] =>
    test(TopNOneHotEncoder("tn1h", 10, eps = 0.01, seed = 1), xs)
  }

  property("delta") = Prop.forAll { xs: List[String] =>
    test(TopNOneHotEncoder("tn1h", 10, delta = 0.01, seed = 1), xs)
  }

  property("encodeMissingValue") = Prop.forAll { xs: List[String] =>
    test(TopNOneHotEncoder("tn1h", 10, delta = 0.01, seed = 1, encodeMissingValue = true), xs)
  }

}
