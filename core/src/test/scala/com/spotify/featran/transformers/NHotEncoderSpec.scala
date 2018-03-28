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

import org.scalacheck._

object NHotEncoderSpec extends TransformerProp("NHotEncoder") {

  private implicit val labelArb = Arbitrary {
    Gen.choose(1, 10).flatMap(Gen.listOfN(_, Gen.alphaStr))
  }

  property("default") = Prop.forAll { xs: List[List[String]] =>
    val cats = xs.flatten.distinct.sorted
    val names = cats.map("n_hot_" + _)
    val expected = xs.map(s => cats.map(c => if (s.contains(c)) 1.0 else 0.0))
    val missing = cats.map(_ => 0.0)
    val oob = List((List("s1", "s2"), missing)) // unseen labels
    test(NHotEncoder("n_hot"), xs, names, expected, missing, oob)
  }

  property("missingValueOpt") = Prop.forAll { xs: List[List[String]] =>
    val missingValueToken = MissingValue.missingValueToken
    val cats = (xs.flatten :+ missingValueToken).distinct.sorted
    val names = cats.map("n_hot_" + _)
    val missing = cats.map(c => if (c == missingValueToken) 1.0 else 0.0)
    val expected = xs.map(s => {
      s.size match {
        case 0 => missing
        case _ => cats.map(c => if (s.contains(c)) 1.0 else 0.0)
      }
    })
    val partialMiss = expected(0).zip(missing).map { case (a, b) => a + b }

    // unseen or partially unseen labels
    val oob = List((List("s1", "s2"), missing), ((List("s1", "s2") ++ xs(0), partialMiss)))
    test(NHotEncoder("n_hot", true), xs, names, expected, missing, oob)
  }

}
