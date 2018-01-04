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

import org.scalatest.{FlatSpec, Matchers}

class MDLPDiscretizerTest extends FlatSpec with Matchers {
  import com.spotify.featran.transformers.mdl.TestUtility._

  it should "Run MDLPD on single mpg column in cars data  (maxBins = 10)" in {
    val cars = readCars
    val data = cars.map(v => (v.origin, v.mpg))

    val result = new MDLPDiscretizer(data).discretize(10).sorted

    val expected = List(Double.NegativeInfinity, 16.1, 21.05, 30.95, Double.PositiveInfinity)
    assert(expected.length === result.length)
    expected.zip(result).map{case(e, r) => assert(e === r)}
  }

  it should "Run MDLPD on single mpg column in cars data (maxBins = 2)" in {
    val cars = readCars
    val data = cars.map(v => (v.origin, v.mpg))

    val result = new MDLPDiscretizer(data).discretize(2).sorted
    val expected = List(Double.NegativeInfinity, 21.05, Double.PositiveInfinity)

    assert(expected.length === result.length)
    expected.zip(result).map{case(e, r) => assert(e === r)}
  }

  it should "Run MDLPD with no records" in {
    val data: List[(String, Double)] = Nil
    val result = new MDLPDiscretizer(data).discretize(2).sorted
    val expected = List(Double.NegativeInfinity, Double.PositiveInfinity)
    assert(expected.length === result.length)
  }
}
