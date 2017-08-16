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

object MaxAbsScalerSpec extends TransformerProp("MaxAbsScaler") {

  property("default") = Prop.forAll { xs: List[Double] =>
    val max = xs.map(math.abs).max
    val expected = xs.map(x => Seq(x / max))
    val oob = List((lowerBound(-max), Seq(-1.0)), (upperBound(max), Seq(1.0)))
    test(MaxAbsScaler("max_abs"), xs, Seq("max_abs"), expected, Seq(0.0), oob)
  }

}
