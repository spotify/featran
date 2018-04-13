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

import breeze.linalg._
import org.scalacheck._

object NormalizerSpec extends TransformerProp("Normalizer") {

  property("default") = Prop.forAll(list[Array[Double]].arbitrary, Gen.choose(1.0, 3.0)) {
    (xs, p) =>
      val names = (0 until 10).map("norm_" + _)
      val expected = xs.map { x =>
        val dv = DenseVector(x)
        (dv / norm(dv, p)).data.toSeq
      }
      val missing = (0 until 10).map(_ => 0.0)
      val oob = List((xs.head :+ 1.0, missing)) // vector of different dimension
      test(Normalizer("norm", p), xs, names, expected, missing, oob)
  }

  property("length") = Prop.forAll { xs: List[Array[Double]] =>
    val msg = "requirement failed: Invalid input length, " +
      s"expected: ${xs.head.length + 1}, actual: ${xs.head.length}"
    testException[Array[Double]](Normalizer("norm", 2.0, xs.head.length + 1), xs) { e =>
      e.isInstanceOf[IllegalArgumentException] && e.getMessage == msg
    }
  }

}
