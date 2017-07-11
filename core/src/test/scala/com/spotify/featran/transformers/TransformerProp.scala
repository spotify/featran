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

package com.spotify.featran.transformers

import com.spotify.featran.FeatureSpec
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck._

abstract class TransformerProp(name: String) extends Properties(name) {

  implicit def list[T](implicit arb: Arbitrary[T]): Arbitrary[List[T]] = Arbitrary {
    Gen
      .listOfN(100, arb.arbitrary)
      .suchThat(_.nonEmpty) // workaround for shrinking failure
  }

  implicit val vectorGen: Arbitrary[Array[Double]] = Arbitrary {
    Gen.buildableOfN[Array[Double], Double](10, Arbitrary.arbitrary[Double])
  }

  // Double.NaN != Double.NaN
  // Also map to float to workaround precision error
  private def safeCompare(xs: List[Seq[Double]], ys: List[Seq[Double]]): Boolean = {
    def d2e(x: Double): Either[Int, Float] = if (x.isNaN) Left(0) else Right(x.toFloat)
    xs.map(_.map(d2e)) == ys.map(_.map(d2e))
  }

  def test[T](t: Transformer[T, _, _],
              input: List[T],
              names: Seq[String],
              expected: List[Seq[Double]],
              missing: Seq[Double],
              outOfBoundsElems: List[(T, Seq[Double])] = Nil): Prop = {
    val fsRequired = FeatureSpec.of[T].required(identity)(t)
    val fsOptional = FeatureSpec.of[Option[T]].optional(identity)(t)

    // all values present
    val f1 = fsRequired.extract(input)
    // add one missing value
    val f2 = fsOptional.extract(input.map(Some(_)) :+ None)

    // extract with settings from a previous session
    val settings = f1.featureSettings
    // first half of the values
    val f3 = fsRequired.extractWithSettings(input.take(input.size / 2), settings)
    // all values plus optional elements out of bound of the previous session
    val f4 = fsRequired.extractWithSettings(outOfBoundsElems.map(_._1), settings)

    Prop.all(
      "f1 names" |: f1.featureNames == List(names),
      "f2 names" |: f2.featureNames == List(names),
      "f3 names" |: f3.featureNames == List(names),
      "f4 names" |: f4.featureNames == List(names),
      "f1 values" |: safeCompare(f1.featureValues[Seq[Double]], expected),
      "f2 values" |: safeCompare(f2.featureValues[Seq[Double]], expected :+ missing),
      "f3 values" |: safeCompare(f3.featureValues[Seq[Double]], expected.take(input.size / 2)),
      "f4 values" |: safeCompare(f4.featureValues[Seq[Double]], outOfBoundsElems.map(_._2)),
      "f2 settings" |: settings == f2.featureSettings,
      "f3 settings" |: settings == f3.featureSettings,
      "f4 settings" |: settings == f4.featureSettings)
  }

  def testException[T](t: Transformer[T, _, _], input: List[T])(p: Throwable => Boolean): Prop =
    try {
      FeatureSpec.of[T].required(identity)(t).extract(input).featureValues[Seq[Double]]
      false
    } catch {
      case e: Exception => p(e)
      case e: Throwable => throw e
    }

}
