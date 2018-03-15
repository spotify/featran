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

import org.scalacheck.{Arbitrary, Gen, Prop}

object PositionEncoderSpec extends TransformerProp("PositionEncoder") {

  private implicit val labelArb = Arbitrary(Gen.alphaStr)

  property("default") = Prop.forAll { xs: List[String] =>
    val cats = xs.distinct.sorted
    val expected =
      xs.map(s => Seq(cats.zipWithIndex.find(c => s == c._1).map(_._2).getOrElse(0).toDouble))
    val oob = List(("s1", Seq(0.0)), ("s2", Seq(0.0))) // unseen labels
    test(PositionEncoder("position"), xs, List("position"), expected, Seq(0.0), oob)
  }

}
