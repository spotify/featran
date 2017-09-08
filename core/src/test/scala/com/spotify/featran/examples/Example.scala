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

package com.spotify.featran.examples

import breeze.linalg._
import com.spotify.featran._
import com.spotify.featran.converters._
import com.spotify.featran.transformers._
import org.scalacheck._

object Example {

  case class Record(b: Boolean, f: Float, d1: Double, d2: Option[Double], d3: Double,
                    s1: String, s2: List[String])

  // Random generator for Record
  val recordGen: Gen[Record] = for {
    b <- Arbitrary.arbitrary[Boolean]
    f <- Arbitrary.arbitrary[Float]
    d1 <- Arbitrary.arbitrary[Double]
    d2 <- Arbitrary.arbitrary[Option[Double]]
    d3 <- Gen.choose(0, 24)
    s1 <- Gen.alphaStr.map(_.take(5))
    n <- Gen.choose(0, 10)
    s2 <- Gen.listOfN(n, Gen.alphaStr.map(_.take(5)))
  } yield Record(b, f, d1, d2, d3, s1, s2)

  // Random generator for Seq[Record]
  val recordsGen: Gen[List[Record]] = Gen.listOfN(20, recordGen)

  private def toArray(r: Record): Array[Double] =
    Array(r.b.asDouble, r.f.toDouble, r.d1, r.d2.getOrElse(0.0))

  // scalastyle:off regex
  // scalastyle:off method.length
  def main(args: Array[String]): Unit = {
    // Random input
    val records = recordsGen.sample.get

    // Start building a feature specification
    val spec = FeatureSpec.of[Record]
      // Required field with Boolean to Double converter from `com.spotify.featran.converters._`
      // Pass value through with Identity transformer
      .required(_.b.asDouble)(Identity("id1"))
      // Requird field with Float to Double conversion
      .required(_.f.toDouble)(Identity("id2"))
      // Vector Identity
      .required(v => Seq(v.f.toDouble))(VectorIdentity("vec_id", 1))
      // Binarize with default threshold 0.0
      .required(_.d1)(Binarizer("bin1"))
      // Binarize with custom threshold
      .required(_.d1)(Binarizer("bin2", threshold = 0.5))
      // Optional field with default missing value None
      // Bucketize into 3 bins
      .optional(_.d2)(Bucketizer("bucket1", Array(0.0, 10.0, 20.0, 30.0)))
      // Optional field with custom missing value
      .optional(_.d2, Some(10.0))(Bucketizer("bucket2", Array(0.0, 10.0, 20.0, 30.0)))
      // Scale by absolution max value
      .required(_.d1)(MaxAbsScaler("abs"))
      // Scale between default min 0.0 and max 1.0
      .required(_.d1)(MinMaxScaler("min_max1"))
      // Scale between custom min and max
      .required(_.d1)(MinMaxScaler("min_max2", 0.0, 100.0))
      // Evaluate von Mises distribution (mu = d3, kappa = 2) at values 0, 4, .., 24
      // (rescaled using scale=pi/12 to be in the interval [0,2pi])
      .required(_.d3)(VonMisesEvaluator("von_mises", 2.0, math.Pi/12,
        Array(0.0, 4.0, 8.0, 12.0, 16.0, 20.0, 24.0)))
      .required(_.s1)(OneHotEncoder("one_hot"))
      // Normalize vector with default p 2.0
      .required(toArray)(Normalizer("norm1"))
      // Normalize vector with custom p
      .required(toArray)(Normalizer("norm2", 3.0))
      .required(_.s2)(NHotEncoder("n_hot"))
      // Same as above but with weighted names
      .required(_.s2.map(s => WeightedLabel(s, 0.5)))(NHotWeightedEncoder("n_hot_weighted"))
      // Record to Array[Double] composite feature
      // Polynomial expansion with default degree 2
      .required(toArray)(PolynomialExpansion("poly1"))
      // Polynomial expansion with custom degree
      .required(toArray)(PolynomialExpansion("poly2", 3))
      // Transform to absolute value first since QuantileDiscretizer requires non-negative value
      // Discretize into 4 quantiles
      .required(x => math.abs(x.d1))(QuantileDiscretizer("quantile", 4))
      // Standard score with default withStd = true and withMean = false
      .required(_.d1)(StandardScaler("std1"))
      // Standard score with custom settings
      .required(_.d1)(StandardScaler("std2", withStd = false, withMean = true))

    // Extract features from Seq[Record]
    val f1 = spec.extract(records)

    println(f1.featureNames.head)
    f1.featureValues[Seq[Double]].foreach(println)

    // Get feature values in different types
    val floatA = f1.featureValues[Array[Float]]
    val doubleA = f1.featureValues[Array[Double]]
    val floatDV = f1.featureValues[DenseVector[Float]]
    val doubleDV = f1.featureValues[DenseVector[Double]]
    val floatSV = f1.featureValues[SparseVector[Float]]
    val doubleSV = f1.featureValues[SparseVector[Double]]

    // Get feature values as above with rejections and the original input record
    val doubleAResults = f1.featureResults[Array[Double]]
    val doubleAValues = doubleAResults.map(_.value)
    val doubleARejections = doubleAResults.map(_.rejections)
    val doubleAOriginals = doubleAResults.map(_.original)

    // Extract settings as a JSON string
    val settings = f1.featureSettings
    println(settings.head)

    // Extract features from new records, but reuse previously saved settings
    val f2 = spec.extractWithSettings(recordsGen.sample.get, settings)

    // Filter out results with rejections and extract valid values
    val validValues = f2.featureResults[Seq[Double]].filter(_.rejections.isEmpty).map(_.value)
  }
  // scalastyle:on method.length
  // scalastyle:on regex

}
