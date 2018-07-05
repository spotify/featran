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

object BucketizerSpec extends TransformerProp("Bucketizer") {

  private val SplitsGen =
    Gen.choose(3, 10).flatMap(n => Gen.listOfN(n, Arbitrary.arbitrary[Double]))

  property("default") = Prop.forAll(list[Double].arbitrary, SplitsGen) { (xs, sp) =>
    test(xs, sp.toArray.sorted)
  }

  // last bucket should be inclusive
  property("inclusive") = Prop.forAll { xs: List[Double] =>
    val (l, u) = (xs.min, xs.max)
    val m = l / 2 + u / 2 // (l + u) might overflow
    val splits = Array(l, m, u)
    test(xs, splits)
  }

  private def test(xs: List[Double], splits: Array[Double]): Prop = {
    val upper = splits.last
    val names = (0 until splits.length - 1).map("bucketizer_" + _)
    val missing = (0 until splits.length - 1).map(_ => 0.0)
    val expected = xs.map { x =>
      val offset =
        if (x == upper) splits.length - 2 else splits.indexWhere(x < _) - 1
      if (offset >= 0) {
        (0 until splits.length - 1).map(i => if (i == offset) 1.0 else 0.0)
      } else {
        missing
      }
    }
    val rejections =
      xs.zip(expected).filter(x => x._1 < splits.head || x._1 > splits.last).map(_._2)
    val oob =
      List((lowerBound(splits.min), missing), (upperBound(splits.max), missing))
    test(Bucketizer("bucketizer", splits), xs, names, expected, missing, oob, rejections)
  }

}
