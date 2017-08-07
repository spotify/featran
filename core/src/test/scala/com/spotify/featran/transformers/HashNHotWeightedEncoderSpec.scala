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
import org.scalacheck.{Arbitrary, Gen, Prop}

object HashNHotWeightedEncoderSpec extends TransformerProp("HashNHotWeightedEncoder") {
  private implicit val weightedVectors = Arbitrary {
    val weightedValueGen = for {
      value <- Gen.chooseNum(-1.0, 1.0)
      n <- Gen.alphaStr
    } yield WeightedLabel(n, value)
    Gen.choose(1, 5).flatMap(Gen.listOfN(_, weightedValueGen))
  }

  private def estimateSize(xs: List[List[WeightedLabel]]): Int = {
    val m = new HyperLogLogMonoid(12)
    xs.flatten.map(_.name).map(m.toHLL(_)).reduce(m.plus).estimatedSize.toInt
  }

  property("default") = Prop.forAll { xs: List[List[WeightedLabel]] =>
    val size = estimateSize(xs)
    val cats = 0 until size
    val names = cats.map("n_hot_" + _)
    val expected = xs.map{ s =>
      val hashes = s.map(x => (HashEncoder.bucket(x.name, size), x.value))
        .groupBy(_._1).map(l => (l._1, l._2.map(_._2).sum))
      cats.map(c => hashes.get(c) match {
        case Some(v) => v
        case None => 0.0
      })
    }
    val missing = cats.map(_ => 0.0)
    test[Seq[WeightedLabel]](HashNHotWeightedEncoder("n_hot"), xs, names, expected, missing)
  }

  property("size") = Prop.forAll { xs: List[List[WeightedLabel]] =>
    val size = 100
    val cats = 0 until size
    val names = cats.map("n_hot_" + _)
    val expected = xs.map{ s =>
      val hashes = s.map(x => (HashEncoder.bucket(x.name, size), x.value))
        .groupBy(_._1).map(l => (l._1, l._2.map(_._2).sum))
      cats.map(c => hashes.get(c) match {
        case Some(v) => v
        case None => 0.0
      })
    }
    val missing = cats.map(_ => 0.0)
    test[Seq[WeightedLabel]](HashNHotWeightedEncoder("n_hot", size), xs, names, expected, missing)
  }
}
