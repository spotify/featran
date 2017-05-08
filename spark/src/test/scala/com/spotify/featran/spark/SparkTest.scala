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

import com.spotify.featran.{FeatureSpec, Transformers}
import org.apache.spark.SparkContext
import org.scalatest._

class SparkTest extends FlatSpec with Matchers {

  private val data = Seq("a", "b", "c", "d", "e") zip Seq(0, 1, 2, 3, 4)

  "FeatureSpec" should "work with Spark" in {
    val sc = new SparkContext("local[4]", "test")
    val f = FeatureSpec.of[(String, Int)]
      .required(_._1)(Transformers.oneHotEncoder("one_hot"))
      .required(_._2.toDouble)(Transformers.minMax("min_max"))
      .extract(sc.parallelize(data))
    f.featureNames.collect() shouldBe Array(Seq(
      "one_hot_a",
      "one_hot_b",
      "one_hot_c",
      "one_hot_d",
      "one_hot_e",
      "min_max"))
    f.featureValues[Array[Double]].collect() shouldBe Array(
      Array(1.0, 0.0, 0.0, 0.0, 0.0, 0.00),
      Array(0.0, 1.0, 0.0, 0.0, 0.0, 0.25),
      Array(0.0, 0.0, 1.0, 0.0, 0.0, 0.50),
      Array(0.0, 0.0, 0.0, 1.0, 0.0, 0.75),
      Array(0.0, 0.0, 0.0, 0.0, 1.0, 1.00))
    sc.stop()
  }
}
