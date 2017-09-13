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

package com.spotify.featran.scalding

import com.spotify.featran._
import com.twitter.scalding._
import org.scalatest._

class ScaldingTest extends FlatSpec with Matchers {
  import TypedPipeChecker._
  import Fixtures._
  import FeatureBuilder._

  "FeatureSpec" should "work with Scalding" in {
    val f = testSpec.extract(TypedPipe.from(testData))
    inMemoryToList(f.featureNames) shouldBe Iterable(expectedNames)
    val result = inMemoryToList(f.featureValues[Seq[Double], Double])
    result should contain theSameElementsAs expectedValues
  }

  "FeatureSpec" should "work with Scalding not crossed" in {
    val f = notCrossedTestSpec.extract(TypedPipe.from(testData))
    inMemoryToList(f.featureNames) shouldBe Iterable(notCrossedExpectedNames)
    val result = inMemoryToList(f.featureValues[Seq[Double], Double])
    result should contain theSameElementsAs notCrossedExpectedValues
  }

  it should "work with MultiFeatureSpec" in {
    noException shouldBe thrownBy {
      val f = recordSpec.extract(TypedPipe.from(records))
      inMemoryToList(f.featureNames)
      inMemoryToList(f.featureValues[Seq[Double]])
    }
  }

}
