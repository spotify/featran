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

import com.twitter.algebird._
import org.scalacheck._

object HeavyHittersSpec extends TransformerProp("HeavyHitters") {

  private implicit val labelArb: Arbitrary[String] = Arbitrary {
    val infrequent = Gen.listOfN(50, Gen.alphaStr).flatMap(xs => Gen.oneOf(xs))
    val frequent = Gen.listOfN(5, Gen.alphaStr).flatMap(xs => Gen.oneOf(xs))
    Gen.frequency((1, infrequent), (50, frequent))
  }
  private val seed = 1

  private def test(transformer: Transformer[String, _, _],
                   xs: List[String],
                   count: Int,
                   eps: Double,
                   delta: Double): Prop = {
    val params = SketchMapParams[String](seed, eps, delta, count)(_.getBytes)
    val aggregator = SketchMap.aggregator[String, Long](params)
    val sm =
      xs.map(x => aggregator.prepare((x, 1L))).reduce(aggregator.monoid.plus)
    val m = sm.heavyHitterKeys.zipWithIndex.toMap.mapValues(_ + 1)
    val expected = xs.map { x =>
      m.get(x) match {
        case Some(rank) =>
          Seq(rank.toDouble, params.frequency(x, sm.valuesTable).toDouble)
        case None => Seq(0.0, 0.0)
      }
    }
    val names = Seq("hh_rank", "hh_freq")
    val missing = Seq(0.0, 0.0)
    test(transformer, xs, names, expected, missing)
  }

  property("default") = Prop.forAll { xs: List[String] =>
    test(HeavyHitters("hh", 10, seed = 1), xs, 10, 0.001, 0.001)
  }

  property("count") = Prop.forAll { xs: List[String] =>
    test(HeavyHitters("hh", 100, seed = 1), xs, 100, 0.001, 0.001)
  }

  property("eps") = Prop.forAll { xs: List[String] =>
    test(HeavyHitters("hh", 10, eps = 0.01, seed = 1), xs, 10, 0.01, 0.001)
  }

  property("delta") = Prop.forAll { xs: List[String] =>
    test(HeavyHitters("hh", 10, delta = 0.01, seed = 1), xs, 10, 0.001, 0.01)
  }

}
