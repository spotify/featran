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

object HashNHotWeightedEncoderSpec extends TransformerProp("HashNHotWeightedEncoder") {

  private implicit val weightedVectors = Arbitrary {
    val weightedValueGen = for {
      value <- Gen.chooseNum(-1.0, 1.0)
      n <- Gen.alphaStr
    } yield WeightedLabel(n, value)
    Gen.choose(1, 5).flatMap(Gen.listOfN(_, weightedValueGen))
  }

  private def estimateSize(xs: List[List[WeightedLabel]]): Double = {
    val m = new HyperLogLogMonoid(12)
    xs.flatten.map(_.name).map(m.toHLL(_)).reduce(m.plus).estimatedSize
  }

  override implicit def list[T](implicit arb: Arbitrary[T]): Arbitrary[List[T]] = Arbitrary {
    Gen.listOfN(10, arb.arbitrary).suchThat(_.nonEmpty) // workaround for shrinking failure
  }

  property("default") = Prop.forAll { xs: List[List[WeightedLabel]] =>
    val size = ceil(estimateSize(xs) * 8.0).toInt
    val cats = 0 until size
    test(HashNHotWeightedEncoder("n_hot"), size, xs)
  }

  property("size") = Prop.forAll { xs: List[List[WeightedLabel]] =>
    val size = 100
    test(HashNHotWeightedEncoder("n_hot", size), size, xs)
  }

  property("scaling factor") = Prop.forAll { xs: List[List[WeightedLabel]] =>
    val scalingFactor = 4.0
    val size = ceil(estimateSize(xs) * scalingFactor).toInt
    test(HashNHotWeightedEncoder("n_hot", 0, scalingFactor), size, xs)
  }

  private def test(encoder: Transformer[List[WeightedLabel], _, _],
                   size: Int,
                   xs: List[List[WeightedLabel]]): Prop = {
    val cats = 0 until size
    val names = cats.map("n_hot_" + _)
    val expected = xs.map { s =>
      val hashes = s
        .map(x => (HashEncoder.bucket(x.name, size), x.value))
        .groupBy(_._1)
        .map(l => (l._1, l._2.map(_._2).sum))
      cats.map(c =>
        hashes.get(c) match {
          case Some(v) => v
          case None    => 0.0
      })
    }
    val missing = cats.map(_ => 0.0)
    test(encoder, xs, names, expected, missing)
  }

}
