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

object MinMaxScalerSpec extends TransformerProp("MinMaxScaler") {

  property("default") = Prop.forAll { xs: List[Double] =>
    test(xs, 0.0, 1.0)
  }

  // limit the range of min and max to avoid overflow
  private val minMaxGen = (for {
    min <- Gen.choose(-1000.0, 1000.0)
    range <- Gen.choose(1.0, 2000.0)
  } yield (min, min + range)).suchThat(t => t._2 > t._1)

  property("params") = Prop.forAll(list[Double].arbitrary, minMaxGen) { (xs, p) =>
    val (minP, maxP) = p
    test(xs, minP, maxP)
  }

  private def test(xs: List[Double], minP: Double, maxP: Double): Prop = {
    val (min, max) = (xs.min, xs.max)
    val f = if ((max - min).isPosInfinity) 2.0 else 1.0
    val delta = max / f - min / f
    val expected =
      xs.map(x => Seq((x / f - min / f) / delta * (maxP - minP) + minP))
    val rejected = xs.zip(expected).filter(x => x._1 < xs.min / f || x._1 > xs.max / f).map(_._2)
    val oob = List((lowerBound(min), Seq(minP)), (upperBound(max), Seq(maxP)))
    val t = MinMaxScaler("min_max", minP, maxP)
    test(t, xs, Seq("min_max"), expected, Seq(minP), oob, rejected)
  }

}
