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

class MDLPDiscretizerTest extends FlatSpec with Matchers {

  import com.spotify.featran.transformers.mdl.TestUtility._

  "MDLPDiscretizer" should "work with cars data (maxBins = 10)" in {
    val data = Cars.map(v => (v.origin, v.mpg))
    val result = new MDLPDiscretizer(data).discretize(10).sorted
    val expected =
      List(Double.NegativeInfinity, 16.1, 21.05, 30.95, Double.PositiveInfinity)
    result.length shouldBe expected.length
    result.zip(expected).map { case (r, e) => r shouldEqual e }
  }

  it should "work with cars data (maxBins = 2)" in {
    val data = Cars.map(v => (v.origin, v.mpg))
    val result = new MDLPDiscretizer(data).discretize(2).sorted
    val expected = List(Double.NegativeInfinity, 21.05, Double.PositiveInfinity)
    result.length shouldBe expected.length
    result.zip(expected).map { case (r, e) => r shouldEqual e }
  }

  it should "work with empty data" in {
    val data = List.empty[(String, Double)]
    val result = new MDLPDiscretizer(data).discretize(2).sorted
    val expected = List(Double.NegativeInfinity, Double.PositiveInfinity)
    result.length shouldBe expected.length
  }

}
