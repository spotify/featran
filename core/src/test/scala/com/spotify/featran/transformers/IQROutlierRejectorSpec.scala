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
import org.scalacheck.{Arbitrary, Gen, Prop}

object IQROutlierRejectorSpec extends TransformerProp("IQROutlierRejector") {

  private implicit val arbPosDouble = Arbitrary(Gen.posNum[Double])

  def lowerUpper(xs: List[Double], numBuckets: Int): (Double, Double) = {
    val qt = xs.map(QTree(_)).reduce(new QTreeSemigroup[Double](QTreeAggregator.DefaultK).plus)
    val (lq, _) = qt.quantileBounds(0.75)
    val (_, fq) = qt.quantileBounds(0.25)
    val iqr = lq - fq
    val l = fq - (iqr * 1.5)
    val u = lq - (iqr * 1.5)
    (l, u)
  }

  property("default") = Prop.forAll(list[Double].arbitrary) { xs =>
    val (l, u) = lowerUpper(xs, 4)
    val rejected = xs.filter(_ => xs.min < xs.max).filter(x => x > u || x < l).map(_ => Seq(0D))
    // records that are not within bounds should always be rejected
    val oob = List((lowerBound(xs.min), Seq(0D)), (upperBound(xs.max), Seq(0D)))
    val r = IQROutlierRejector("iqr")
    test(r, xs, Seq("iqr"), xs.map(_ => Seq(0D)), Seq(0.0), oob, rejected)
  }

  property("rejectLower don't rejectUpper") = Prop.forAll(list[Double].arbitrary) { xs =>
    val (l, _) = lowerUpper(xs, 4)
    val rejected =
      xs.filter(_ => xs.min < xs.max).filter(_ < l).map(_ => Seq(0D))
    val r = IQROutlierRejector("iqr", rejectLower = true, rejectUpper = false)
    test(r, xs, Seq("iqr"), xs.map(_ => Seq(0D)), Seq(0.0), rejected = rejected)
  }

  property("rejectUpper don't rejectLower") = Prop.forAll(list[Double].arbitrary) { xs =>
    val (_, u) = lowerUpper(xs, 4)
    val rejected =
      xs.filter(_ => xs.min < xs.max).filter(_ > u).map(_ => Seq(0D))
    val r = IQROutlierRejector("iqr", rejectLower = false, rejectUpper = true)
    test(r, xs, Seq("iqr"), xs.map(_ => Seq(0D)), Seq(0.0), rejected = rejected)
  }

}
