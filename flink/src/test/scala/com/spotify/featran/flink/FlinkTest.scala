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

package com.spotify.featran.flink

import com.spotify.featran._
import org.apache.flink.api.scala._
import org.scalatest._

class FlinkTest extends FlatSpec with Matchers {

  import Fixtures._

  "Flink" should "work with FeatureSpec" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val f = TestSpec.extract(env.fromCollection(TestData))
    f.featureNames.collect() shouldBe Seq(ExpectedNames)
    f.featureValues[Seq[Double]].collect() should contain theSameElementsAs ExpectedValues
  }

  it should "work with MultiFeatureSpec" in {
    noException shouldBe thrownBy {
      val env = ExecutionEnvironment.getExecutionEnvironment
      val f = RecordSpec.extract(env.fromCollection(Records))
      f.featureNames.collect()
      f.featureValues[Seq[Double]].collect()
    }
  }

}
