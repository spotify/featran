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

object BinarizerSpec extends TransformerProp("Binarizer") {
  property("default") = Prop.forAll { (xs: List[Double]) =>
    val expected = xs.map(x => Seq(if (x > 0.0) 1.0 else 0.0))
    test(Binarizer("id"), xs, Seq("id"), expected, Seq(0.0))
  }

  property("threshold") = Prop.forAll { (xs: List[Double], threshold: Double) =>
    val expected = xs.map(x => Seq(if (x > threshold) 1.0 else 0.0))
    test(Binarizer("id", threshold), xs, Seq("id"), expected, Seq(0.0))
  }
}
