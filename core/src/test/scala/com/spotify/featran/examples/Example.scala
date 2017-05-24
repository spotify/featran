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

  case class Record(b: Boolean, f: Float, d1: Double, d2: Option[Double],
                    s1: String, s2: List[String])

  // Random generator for Record
  val recordGen: Gen[Record] = for {
    b <- Arbitrary.arbitrary[Boolean]
    f <- Arbitrary.arbitrary[Float]
    d1 <- Arbitrary.arbitrary[Double]
    d2 <- Arbitrary.arbitrary[Option[Double]]
    s1 <- Gen.alphaStr.map(_.take(5))
    n <- Gen.choose(0, 10)
    s2 <- Gen.listOfN(n, Gen.alphaStr.map(_.take(5)))
  } yield Record(b, f, d1, d2, s1, s2)

  // Random generator for Seq[Record]
  val recordsGen: Gen[List[Record]] = Gen.listOfN(20, recordGen)

  // scalastyle:off method.length
  def main(args: Array[String]): Unit = {
    // Random input
    val records = recordsGen.sample.get

    // Start building a feature specification
    val f = FeatureSpec.of[Record]
      // Required field with Boolean to Double converter from `com.spotify.featran.converters._`
      // Pass value through with Identity transformer
      .required(_.b.asDouble)(Identity("id1"))
      // Requird field with Float to Double conversion
      .required(_.f.toDouble)(Identity("id2"))
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
      .required(_.s1)(OneHotEncoder("one_hot"))
      .required(_.s2)(NHotEncoder("n_hot"))
      // Record to Array[Double] composite feature
      // Polynomial expansion with default degree 2
      .required {
        r => Array(r.b.asDouble, r.f.toDouble, r.d1, r.d2.getOrElse(0.0))
      } (PolynomialExpansion("poly1"))
      // Polynomial expansion with custom degree
      .required {
        r => Array(r.b.asDouble, r.f.toDouble, r.d1, r.d2.getOrElse(0.0))
      } (PolynomialExpansion("poly2", 3))
      // Transform to absolute value first since QuantileDiscretizer requires non-negative value
      // Discretize into 4 quantiles
      .required(x => math.abs(x.d1))(QuantileDiscretizer("quantile", 4))
      // Standard score with default withStd = true and withMean = false
      .required(_.d1)(StandardScaler("std1"))
      // Standard score with custom settings
      .required(_.d1)(StandardScaler("std2", withStd = false, withMean = true))
      // Extract features from Seq[Record]
      .extract(records)

    // scalastyle:off regex
    println(f.featureNames.head)
    f.featureValues[Seq[Double]].foreach(println)
    // scalastyle:on regex

    // Get feature values in different types
    val floatA = f.featureValues[Array[Float]]
    val doubleA = f.featureValues[Array[Double]]
    val floatDV = f.featureValues[DenseVector[Float]]
    val doubleDV = f.featureValues[DenseVector[Double]]
    val floatSV = f.featureValues[SparseVector[Float]]
    val doubleSV = f.featureValues[SparseVector[Double]]
  }
  // scalastyle:on method.length

}
