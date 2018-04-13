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

package com.spotify.featran.transformers.mdl

import org.scalatest._

class ThresholdFinderTest extends FlatSpec with Matchers {

  "ThresholdFinder" should "work with nLabels = 3 and feature size = 4" in {
    val finder =
      new ThresholdFinder(nLabels = 3, stoppingCriterion = 0, maxBins = 100, minBinWeight = 1)

    val feature = Array((5.0f, Array(1L, 2L, 3L)),
                        (4.0f, Array(5L, 4L, 20L)),
                        (3.5f, Array(3L, 20L, 12L)),
                        (3.0f, Array(8L, 18L, 2L)))

    val result = finder.findThresholds(feature)
    result shouldBe Seq(Float.NegativeInfinity, 4.0, Float.PositiveInfinity)
  }

  it should "work with duplicates" in {
    val finder =
      new ThresholdFinder(nLabels = 3, stoppingCriterion = 0, maxBins = 100, minBinWeight = 1)

    val best =
      finder.bestThreshold(List((1.0f, Array.empty, Array.empty, Array.empty)),
                           Some(1.0f),
                           Array.empty)
    best shouldBe empty
  }

}
