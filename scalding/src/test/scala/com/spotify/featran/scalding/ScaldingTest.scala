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
import com.spotify.featran.transformers._
import com.twitter.scalding._
import org.scalatest._

class ScaldingTest extends FlatSpec with Matchers {

  private val data = Seq("a", "b", "c", "d", "e") zip Seq(0, 1, 2, 3, 4)

  def materialize[T](p: TypedPipe[T]): Iterable[T] =
    p.toIterableExecution.waitFor(Config.default, Local(true)).get

  "FeatureSpec" should "work with Scalding" in {
    val f = FeatureSpec.of[(String, Int)]
      .required(_._1)(OneHotEncoder("one_hot"))
      .required(_._2.toDouble)(MinMaxScaler("min_max"))
      .extract(TypedPipe.from(data))
    materialize(f.featureNames) shouldBe Iterable(Seq(
      "one_hot_a",
      "one_hot_b",
      "one_hot_c",
      "one_hot_d",
      "one_hot_e",
      "min_max"))
    materialize(f.featureValues[Array[Double]]) should contain theSameElementsAs Iterable(
      Array(1.0, 0.0, 0.0, 0.0, 0.0, 0.00),
      Array(0.0, 1.0, 0.0, 0.0, 0.0, 0.25),
      Array(0.0, 0.0, 1.0, 0.0, 0.0, 0.50),
      Array(0.0, 0.0, 0.0, 1.0, 0.0, 0.75),
      Array(0.0, 0.0, 0.0, 0.0, 1.0, 1.00))
  }
}
