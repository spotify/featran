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

object StandardScalerSpec extends TransformerProp("StandardScaler") {
  def meanAndStddev(xs: List[Double]): (Double, Double) = {
    // breeze.stats.stddev is sample stddev
    val mean = xs.map(_ / xs.length).sum
    (mean, math.sqrt(xs.map(x => math.pow(x - mean, 2)).sum / xs.size))
  }

  property("default") = Prop.forAll { (xs: List[Double]) =>
    val (mean, stddev) = meanAndStddev(xs)
    val expected = xs.map(x => Seq((x - mean) / stddev + mean))
    test(StandardScaler("std"), xs, Seq("std"), expected, Seq(mean))
  }

  property("withStd withMean") = Prop.forAll { (xs: List[Double]) =>
    val (mean, stddev) = meanAndStddev(xs)
    val expected = xs.map(x => Seq((x - mean) / stddev))
    val (withStd, withMean) = (true, true)
    test(StandardScaler("std", withStd, withMean), xs, Seq("std"), expected, Seq(0.0))
  }

  property("withStd withoutMean") = Prop.forAll { (xs: List[Double]) =>
    val (mean, stddev) = meanAndStddev(xs)
    val expected = xs.map(x => Seq((x - mean) / stddev + mean))
    val (withStd, withMean) = (true, false)
    test(StandardScaler("std", withStd, withMean), xs, Seq("std"), expected, Seq(mean))
  }

  property("withoutStd withMean") = Prop.forAll { (xs: List[Double]) =>
    val (mean, _) = meanAndStddev(xs)
    val expected = xs.map(x => Seq(x - mean))
    val (withStd, withMean) = (false, true)
    test(StandardScaler("std", withStd, withMean), xs, Seq("std"), expected, Seq(0.0))
  }

  property("withoutStd withoutMean") = Prop.forAll { (xs: List[Double]) =>
    val (mean, _) = meanAndStddev(xs)
    val expected = xs.map(Seq(_))
    val (withStd, withMean) = (false, false)
    test(StandardScaler("std", withStd, withMean), xs, Seq("std"), expected, Seq(mean))
  }
}
