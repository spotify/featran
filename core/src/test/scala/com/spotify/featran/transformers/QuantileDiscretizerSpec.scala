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

import java.util.{TreeMap => JTreeMap}

import com.twitter.algebird.{QTree, QTreeAggregator, QTreeSemigroup}
import org.scalacheck._

object QuantileDiscretizerSpec extends TransformerProp("QuantileDiscretizer") {

  private implicit val arbPosDouble = Arbitrary(Gen.posNum[Double])

  property("default") = Prop.forAll(list[Double].arbitrary, Gen.oneOf(2, 4, 5)) {
    (xs, numBuckets) =>
      // FIXME: make this a black box
      val qt = xs.map(QTree(_)).reduce(new QTreeSemigroup[Double](QTreeAggregator.DefaultK).plus)
      val m = new JTreeMap[Double, Int]()
      val interval = 1.0 / numBuckets
      for (i <- 1 until numBuckets) {
        val (l, u) = qt.quantileBounds(interval * i)
        val k = l / 2 + u / 2 // (l + u) might overflow
        if (!m.containsKey(k)) {
          m.put(k, i - 1)
        }
      }
      m.put(qt.upperBound, numBuckets - 1)
      val expected = xs.map { x =>
        (0 until numBuckets).map(i => if (i == m.higherEntry(x).getValue) 1.0 else 0.0)
      }
      val rejected = xs
        .zip(expected)
        .filter(x => xs.min == xs.max || x._1 < qt.lowerBound || x._1 > qt.upperBound)
        .map(_._2)
      val names = (0 until numBuckets).map("quantile_" + _)
      val missing = (0 until numBuckets).map(_ => 0.0)
      val oob = List((lowerBound(xs.min), 1.0 +: (0 until numBuckets - 1).map(_ => 0.0)),
                     (upperBound(xs.max), (0 until numBuckets - 1).map(_ => 0.0) :+ 1.0))
      test(QuantileDiscretizer("quantile", numBuckets), xs, names, expected, missing, oob)
  }

}
