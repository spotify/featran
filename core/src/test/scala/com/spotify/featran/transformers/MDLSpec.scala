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

import com.spotify.featran.transformers.mdl.MDLPDiscretizer
import org.scalacheck._

class MDLSpec extends TransformerProp("MDL") {

  private implicit val arbPosDouble = Arbitrary(Gen.posNum[Double])

  private implicit val mdlRecords = Arbitrary {
    for {
      value <- Gen.oneOf(Seq("1", "2", "3"))
      n <- Gen.posNum[Double]
    } yield MDLRecord(value, n)
  }

  property("default") = Prop.forAll { xs: List[MDLRecord[String]] =>
    val ranges = new MDLPDiscretizer[String](xs.map(l => (l.label, l.value))).discretize()

    val slices = ranges.tail
    val names = slices.indices.map(idx => s"mdl_$idx")

    val expected = xs.map{case MDLRecord(_, x) =>
      val array = Array.fill[Double](slices.size)(0.0)
      val bin = slices
        .zipWithIndex
        .find(_._1.toDouble > x)
        .map(_._2)
        .getOrElse(slices.length-1)

      array(bin) = 1.0
      array.toList
    }

    val missing = slices.indices.map(_ => 0.0)
    test[MDLRecord[String]](MDL[String]("mdl"), xs, names, expected, missing)
  }
}
