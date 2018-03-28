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
  private val seed = 1

  def getExpectedOutputVector(missingValueOpt: Option[String],
                              s: String,
                              cats: List[String]): Seq[Double] = {
    val v: Seq[Double] = cats.map(c => if (s == c) 1.0 else 0.0)
    missingValueOpt match {
      case Some(missingValueToken) => {
        v.sum match {
          case nonZero if nonZero > 0 => v
          case _ => cats.map(c => if (c == missingValueToken) 1.0 else 0.0)
        }
      }
      case None => v
    }
  }

  private def test(transformer: Transformer[String, _, _], xs: List[String],
                   count: Int, eps: Double, delta: Double,
                   missingValueOpt: Option[String]): Prop = {
    val params = SketchMapParams[String](seed, eps, delta, count)(_.getBytes)
    val aggregator = SketchMap.aggregator[String, Long](params)
    val sm = xs.map(x => aggregator.prepare((x, 1L))).reduce(aggregator.monoid.plus)
    val cats = missingValueOpt match {
      case Some(missingValueToken) => (sm.heavyHitterKeys :+ missingValueToken).sorted
      case _ => sm.heavyHitterKeys.sorted
    }
    val names = cats.map("tn1h_" + _)
    val expected = xs.map(s => getExpectedOutputVector(missingValueOpt, s, cats))
    val missing = missingValueOpt match {
      case Some(missingValueToken) => cats.map(c => if (c == missingValueToken) 1.0 else 0.0)
      case None => cats.map(_ => 0.0)
    }
    val oob = List(("s1", missing), ("s2", missing)) // unseen labels
    val rejected = xs.flatMap(x => if (cats.contains(x)) None else Some(missing))

    test(transformer, xs, names, expected, missing, oob, rejected)
  }

  property("default") = Prop.forAll { xs: List[String] =>
    test(TopNOneHotEncoder("tn1h", 10, seed = 1), xs, 10, 0.001, 0.001, None)
  }

  property("count") = Prop.forAll { xs: List[String] =>
    test(TopNOneHotEncoder("tn1h", 100, seed = 1), xs, 100, 0.001, 0.001, None)
  }

  property("eps") = Prop.forAll { xs: List[String] =>
    test(TopNOneHotEncoder("tn1h", 10, eps = 0.01, seed = 1), xs, 10, 0.01, 0.001, None)
  }

  property("delta") = Prop.forAll { xs: List[String] =>
    test(TopNOneHotEncoder("tn1h", 10, delta = 0.01, seed = 1), xs, 10, 0.001, 0.01, None)
  }

  property("encodeMissingValue") = Prop.forAll { xs: List[String] =>
    val missingTokenOpt = Some(MissingValue.missingValueToken)
    test(TopNOneHotEncoder("tn1h", 10, delta = 0.01, seed = 1, encodeMissingValue = true),
      xs, 10, 0.001, 0.01, missingTokenOpt)
  }

}
