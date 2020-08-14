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

import com.twitter.algebird.HyperLogLogMonoid
import org.scalacheck._

import scala.math.ceil

object HashOneHotEncoderSpec extends TransformerProp("HashOneHotEncoder") {
  implicit private val labelArb: Arbitrary[String] = Arbitrary(Gen.alphaStr)

  private def estimateSize(xs: List[String]): Double = {
    val m = new HyperLogLogMonoid(12)
    xs.map(m.toHLL(_)).reduce(m.plus).estimatedSize
  }

  property("default") = Prop.forAll { xs: List[String] =>
    val size = ceil(estimateSize(xs) * 8.0).toInt
    test(HashOneHotEncoder("one_hot"), size, xs)
  }

  property("size") = Prop.forAll { xs: List[String] =>
    val size = 100
    test(HashOneHotEncoder("one_hot", size), size, xs)
  }

  property("scaling factor") = Prop.forAll { xs: List[String] =>
    val scalingFactor = 2.0
    val size = ceil(estimateSize(xs) * scalingFactor).toInt
    test(HashOneHotEncoder("one_hot", 0, scalingFactor), size, xs)
  }

  private def test(encoder: Transformer[String, _, _], size: Int, xs: List[String]): Prop = {
    val cats = 0 until size
    val names = cats.map("one_hot_" + _)
    val expected = xs.map(s => cats.map(c => if (HashEncoder.bucket(s, size) == c) 1.0 else 0.0))
    val missing = cats.map(_ => 0.0)
    test(encoder, xs, names, expected, missing)
  }
}
