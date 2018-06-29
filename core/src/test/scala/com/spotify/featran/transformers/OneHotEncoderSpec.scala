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

object OneHotEncoderSpec extends TransformerProp("OneHotEncoder") {

  private implicit val labelArb = Arbitrary(Gen.alphaStr)

  property("default") = Prop.forAll { xs: List[String] =>
    val cats = xs.distinct.sorted
    val names = cats.map("one_hot_" + _)
    val expected = xs.map(s => cats.map(c => if (s == c) 1.0 else 0.0))
    val missing = cats.map(_ => 0.0)
    val oob = List(("s1", missing), ("s2", missing)) // unseen labels
    test(OneHotEncoder("one_hot"), xs, names, expected, missing, oob)
  }

  property("encodeMissingValue") = Prop.forAll { xs: List[String] =>
    import MissingValue.MissingValueToken
    val cats = xs.distinct.sorted :+ MissingValueToken
    val names = cats.map("one_hot_" + _)
    val expected = xs.map(s => cats.map(c => if (s == c) 1.0 else 0.0))
    val missing = cats.map(c => if (c == MissingValueToken) 1.0 else 0.0)
    val oob = List(("s1", missing), ("s2", missing)) // unseen labels
    test(OneHotEncoder("one_hot", encodeMissingValue = true), xs, names, expected, missing, oob)
  }
}
