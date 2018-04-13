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

import scala.util.Try

object PolynomialExpansionSpec extends TransformerProp("PolynomialExpansion") {

  property("default") = Prop.forAll(list[Array[Double]].arbitrary, Gen.choose(2, 4)) {
    (xs, degree) =>
      val dim = PolynomialExpansion.expand(xs.head, degree).length
      val names = (0 until dim).map("poly_" + _)
      val expected = xs.map(v => PolynomialExpansion.expand(v, degree).toSeq)
      val missing = (0 until dim).map(_ => 0.0)
      val oob = List((xs.head :+ 1.0, missing)) // vector of different dimension
      test(PolynomialExpansion("poly", degree), xs, names, expected, missing, oob)
  }

  property("length") = Prop.forAll { xs: List[Array[Double]] =>
    val msg = "requirement failed: Invalid input length, " +
      s"expected: ${xs.head.length + 1}, actual: ${xs.head.length}"
    testException[Array[Double]](PolynomialExpansion("id", 2, xs.head.length + 1), xs) { e =>
      e.isInstanceOf[IllegalArgumentException] && e.getMessage == msg
    }
  }

  import org.apache.commons.math3.{util => cmu}

  private val genNK = {
    // cover all overflow scenarios
    val genN =
      Gen.frequency((10, Gen.choose(0, 61)), (5, Gen.choose(62, 66)), (1, Gen.choose(67, 70)))
    // n must be >= k
    for {
      n <- genN
      k <- Gen.choose(0, n)
    } yield (n, k)
  }

  property("binomial") = Prop.forAll(genNK) {
    case (n, k) =>
      val actual = Try(CombinatoricsUtils.binomialCoefficient(n, k))
      val expected = Try(cmu.CombinatoricsUtils.binomialCoefficient(n, k))
      actual.toOption == expected.toOption
  }

  property("gcd") = Prop.forAll { (x: Int, y: Int) =>
    val actual = Try(CombinatoricsUtils.gcd(x, y))
    val expected = Try(cmu.ArithmeticUtils.gcd(x, y))
    actual.toOption == expected.toOption
  }

  property("mulAndCheck") = Prop.forAll { (x: Long, y: Long) =>
    val actual = Try(CombinatoricsUtils.mulAndCheck(x, y))
    val expected = Try(cmu.ArithmeticUtils.mulAndCheck(x, y))
    actual.toOption == expected.toOption
  }

  property("abs") = Prop.forAll { x: Int =>
    CombinatoricsUtils.abs(x) == math.abs(x)
  }

}
