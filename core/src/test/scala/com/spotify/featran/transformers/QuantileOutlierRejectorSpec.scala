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

import com.twitter.algebird.{QTree, QTreeAggregator, QTreeSemigroup}
import org.scalacheck._

object QuantileOutlierRejectorSpec extends TransformerProp("QuantileOutlierRejector") {

  private implicit val arbPosDouble = Arbitrary(Gen.posNum[Double])

  def lowerUpper(xs: List[Double], numBuckets: Int): (Double, Double) = {
    val qt = xs.map(QTree(_)).reduce(new QTreeSemigroup[Double](QTreeAggregator.DefaultK).plus)
    val lq = (numBuckets - 1.0) / numBuckets
    val fq = 1.0 / numBuckets
    val (u, _) = qt.quantileBounds(lq)
    val (_, l) = qt.quantileBounds(fq)
    (l, u)
  }

  property("default") = Prop.forAll(list[Double].arbitrary, Gen.oneOf(3 to 20)) {
    (xs, numBuckets) =>
      val (l, u) = lowerUpper(xs, numBuckets)
      val rejected = xs.filter(_ => xs.min < xs.max).filter(x => x > u || x < l).map(_ => Seq(0D))
      // records that are not within bounds should always be rejected
      val oob =
        List((lowerBound(xs.min), Seq(0D)), (upperBound(xs.max), Seq(0D)))
      val r = QuantileOutlierRejector("quantile", numBuckets = numBuckets)
      test(r, xs, Seq("quantile"), xs.map(_ => Seq(0D)), Seq(0.0), oob, rejected)
  }

  property("rejectLower don't rejectUpper") =
    Prop.forAll(list[Double].arbitrary, Gen.oneOf(3 to 20)) { (xs, numBuckets) =>
      val (l, u) = lowerUpper(xs, numBuckets)
      val rejected =
        xs.filter(_ => xs.min < xs.max).filter(_ < l).map(_ => Seq(0D))
      val r = QuantileOutlierRejector("quantile",
                                      rejectLower = true,
                                      rejectUpper = false,
                                      numBuckets = numBuckets)
      test(r, xs, Seq("quantile"), xs.map(_ => Seq(0D)), Seq(0.0), rejected = rejected)
    }

  property("rejectUpper don't rejectLower") =
    Prop.forAll(list[Double].arbitrary, Gen.oneOf(3 to 20)) { (xs, numBuckets) =>
      val (l, u) = lowerUpper(xs, numBuckets)
      val rejected =
        xs.filter(_ => xs.min < xs.max).filter(_ > u).map(_ => Seq(0D))
      val r = QuantileOutlierRejector("quantile",
                                      rejectLower = false,
                                      rejectUpper = true,
                                      numBuckets = numBuckets)
      test(r, xs, Seq("quantile"), xs.map(_ => Seq(0D)), Seq(0.0), rejected = rejected)
    }

}
