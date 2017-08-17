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

package com.spotify.featran

import com.spotify.featran.transformers._

object Fixtures {

  val testData = Seq("a", "b", "c", "d", "e") zip Seq(0, 1, 2, 3, 4)

  val testSpec = FeatureSpec.of[(String, Int)]
    .required(_._1)(OneHotEncoder("one_hot"))
    .required(_._2.toDouble)(MinMaxScaler("min_max"))

  val expectedNames = Seq(
    "one_hot_a",
    "one_hot_b",
    "one_hot_c",
    "one_hot_d",
    "one_hot_e",
    "min_max")

  val expectedValues = Seq(
    Seq(1.0, 0.0, 0.0, 0.0, 0.0, 0.00),
    Seq(0.0, 1.0, 0.0, 0.0, 0.0, 0.25),
    Seq(0.0, 0.0, 1.0, 0.0, 0.0, 0.50),
    Seq(0.0, 0.0, 0.0, 1.0, 0.0, 0.75),
    Seq(0.0, 0.0, 0.0, 0.0, 1.0, 1.00))

  case class Record(x: Double, xo: Option[Double],
                    v: Array[Double], vo: Option[Array[Double]],
                    s1: String, s2: Seq[String], s3: Seq[WeightedLabel])

  val records = (1 to 100).map { x =>
    val d = x.toDouble
    val s = "v" + d
    Record(
      d, Some(d), Array.fill(10)(d), Some(Array.fill(10)(d)),
      s, Seq(s), Seq(WeightedLabel(s, 1.0)))
  }

  private val recordSpec1 = FeatureSpec.of[Record]
    .required(_.x)(Identity("x"))
    .optional(_.xo)(Identity("xo"))
    .required(_.v)(VectorIdentity("v"))
    .optional(_.vo)(VectorIdentity("vo"))

  private val recordSpec2 = FeatureSpec.of[Record]
    .required(_.x)(Binarizer("bin"))
    .required(_.x)(Bucketizer("bucket", Array(0.0, 10.0, 100.0)))
    .required(_.s1)(HashOneHotEncoder("hash-one-hot"))
    .required(_.s2)(HashNHotEncoder("hash-n-hot"))
    .required(_.s3)(HashNHotWeightedEncoder("hash-n-hot-weighted"))
    .required(_.x)(Identity("id"))
    .required(_.x)(MaxAbsScaler("max-abs"))
    .required(_.x)(MinMaxScaler("min-max"))
    .required(_.s2)(NHotEncoder("n-hot"))
    .required(_.s3)(NHotWeightedEncoder("n-hot-weighted"))
    .required(_.v)(Normalizer("norm"))
    .required(_.s1)(OneHotEncoder("one-hot"))
    .required(_.v)(PolynomialExpansion("poly"))
    .required(_.x)(QuantileDiscretizer("quantile"))
    .required(_.x)(StandardScaler("standard"))
    .required(_.v)(VectorIdentity("vec-id"))
    .required(_.x)(VonMisesEvaluator("von-mises", 1.0, 0.01, Array(0.0, 1.0, 2.0)))

  val recordSpec = MultiFeatureSpec(recordSpec1, recordSpec2)

}
