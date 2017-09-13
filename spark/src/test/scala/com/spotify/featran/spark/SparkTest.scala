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

package com.spotify.featran.spark

import com.spotify.featran._
import org.apache.spark.SparkContext
import org.scalatest._

class SparkTest extends FlatSpec with Matchers {

  import Fixtures._
  import FeatureBuilder._

  "FeatureSpec" should "work with Spark" in {
    val sc = new SparkContext("local[4]", "test")
    val f = testSpec.extract(sc.parallelize(testData))
    f.featureNames.collect() shouldBe Array(expectedNames)
    f.featureValues[Seq[Double], Double].collect() should contain theSameElementsAs expectedValues
    sc.stop()
  }

  "FeatureSpec" should "work with not crossed Spark" in {
    val sc = new SparkContext("local[4]", "test")
    val f = notCrossedTestSpec.extract(sc.parallelize(testData))
    f.featureNames.collect() shouldBe Array(notCrossedExpectedNames)
    val results = f.featureValues[Seq[Double], Double].collect()
    results should contain theSameElementsAs notCrossedExpectedValues
    sc.stop()
  }

  it should "work with MultiFeatureSpec" in {
    noException shouldBe thrownBy {
      val sc = new SparkContext("local[4]", "test")
      val f = recordSpec.extract(sc.parallelize(records))
      f.featureNames.collect()
      f.featureValues[Seq[Double]].collect()
      sc.stop()
    }
  }

}
