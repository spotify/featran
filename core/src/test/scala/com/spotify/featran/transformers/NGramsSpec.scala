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

object NGramsSpec extends TransformerProp("NHotEncoder") {

  private implicit val labelArb = Arbitrary {
    Gen.choose(1, 5).flatMap(Gen.listOfN(_, Gen.alphaStr))
  }

  property("default") = Prop.forAll { xs: List[List[String]] =>
    val transformer = new NGrams("n_gram", 2, 4, " ")
    val ngrams = xs.map(transformer.ngrams(_))
    val cats = ngrams.flatten.distinct.sorted
    val names = cats.map("n_gram_" + _)
    val expected =
      ngrams.map(s => cats.map(c => if (s.contains(c)) 1.0 else 0.0))
    val missing = cats.map(_ => 0.0)
    val oob = List((List("s1", "s2"), missing)) // unseen labels
    test(transformer, xs, names, expected, missing, oob)
  }

}
