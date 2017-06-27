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

package com.spotify.featran.scio

import com.spotify.featran._
import com.spotify.featran.transformers._
import com.spotify.scio.testing._

class ScioTest extends PipelineSpec {

  private val data = Seq("a", "b", "c", "d", "e") zip Seq(0, 1, 2, 3, 4)

  "FeatureSpec" should "work with Scio" in {
    runWithContext { sc =>
      val f = FeatureSpec.of[(String, Int)]
        .required(_._1)(OneHotEncoder("one_hot"))
        .required(_._2.toDouble)(MinMaxScaler("min_max"))
        .extract(sc.parallelize(data))
      f.featureNames should containSingleValue (Seq(
        "one_hot_a",
        "one_hot_b",
        "one_hot_c",
        "one_hot_d",
        "one_hot_e",
        "min_max"))
      f.featureValues[Seq[Double]] should containInAnyOrder (Seq(
        Seq(1.0, 0.0, 0.0, 0.0, 0.0, 0.00),
        Seq(0.0, 1.0, 0.0, 0.0, 0.0, 0.25),
        Seq(0.0, 0.0, 1.0, 0.0, 0.0, 0.50),
        Seq(0.0, 0.0, 0.0, 1.0, 0.0, 0.75),
        Seq(0.0, 0.0, 0.0, 0.0, 1.0, 1.00)))
    }
  }

  private class NonSerializable {
    def method(a: String): Double = a.toDouble
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail on throw serializable feature" in {
    runWithContext { sc =>
      val foo = new NonSerializable()
      val f = FeatureSpec.of[(String, Int)]
        .required(e => foo.method(e._1))(Identity("foo"))
        .extract(sc.parallelize(data))

      val thrown = the [RuntimeException] thrownBy f.featureValues[Seq[Double]]
      thrown.getMessage should startWith("unable to serialize anonymous function")
    }
  }
  // scalastyle:on no.whitespace.before.left.bracket

}
