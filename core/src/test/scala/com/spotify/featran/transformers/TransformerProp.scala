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

import breeze.linalg.SparseVector

import scala.collection.Set
import com.spotify.featran.{FeatureSpec, FlatConverter, FlatExtractor, MultiFeatureSpec}
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck._

import scala.reflect.ClassTag

abstract class TransformerProp(name: String) extends Properties(name) {
  import com.spotify.featran.json._

  implicit def list[T](implicit arb: Arbitrary[T]): Arbitrary[List[T]] =
    Arbitrary {
      Gen.listOfN(100, arb.arbitrary).suchThat(_.nonEmpty) // workaround for shrinking failure
    }

  implicit val vectorGen: Arbitrary[Array[Double]] = Arbitrary {
    Gen.buildableOfN[Array[Double], Double](10, Arbitrary.arbitrary[Double])
  }

  // Double.NaN != Double.NaN
  // Also map to float to workaround precision error
  private def d2e(x: Double): Either[Int, Float] =
    if (x.isNaN) Left(0) else Right(x.toFloat)

  private def safeCompare(xs: List[Seq[Double]], ys: List[Seq[Double]]): Boolean =
    xs.map(_.map(d2e)) == ys.map(_.map(d2e))

  // scalastyle:off method.length
  def test[T: ClassTag](t: Transformer[T, _, _],
                        input: List[T],
                        names: Seq[String],
                        expected: List[Seq[Double]],
                        missing: Seq[Double],
                        outOfBoundsElems: List[(T, Seq[Double])] = Nil,
                        rejected: List[Seq[Double]] = Nil): Prop = {
    val fsRequired = FeatureSpec.of[T].required(identity)(t)
    val fsOptional = FeatureSpec.of[Option[T]].optional(identity)(t)

    // all values present
    val f1 = fsRequired.extract(input)
    // all rejected
    val rejections = f1
      .featureResults[Seq[Double]]
      .flatMap(r => if (r.rejections.keySet == Set(t.name)) Some(r.value) else None)
    // add one missing value
    val f2 = fsOptional.extract(input.map(Some(_)) :+ None)

    // extract with settings from a previous session
    val settings = f1.featureSettings
    // first half of the values
    val f3 =
      fsRequired.extractWithSettings(input.take(input.size / 2), settings)
    // all values plus optional elements out of bound of the previous session
    val f4 =
      fsRequired.extractWithSettings(outOfBoundsElems.map(_._1), settings)
    val f4results = f4.featureResults[Seq[Double]]
    val c = f1.featureValues[Seq[Double]]

    val propStandard = Prop.all(
      "f1 names" |: f1.featureNames == List(names),
      "f2 names" |: f2.featureNames == List(names),
      "f3 names" |: f3.featureNames == List(names),
      "f4 names" |: f4.featureNames == List(names),
      "f1 values" |: safeCompare(f1.featureValues[Seq[Double]], expected),
      "f1 sparse values" |: safeCompare(
        f1.featureValues[SparseVector[Double]].map(_.toDenseVector.data.toSeq),
        expected),
      "f1 rejections" |: safeCompare(rejections, rejected),
      "f2 values" |: safeCompare(f2.featureValues[Seq[Double]], expected :+ missing),
      "f3 values" |: safeCompare(f3.featureValues[Seq[Double]], expected.take(input.size / 2)),
      "f4 values" |: safeCompare(f4results.map(_.value), outOfBoundsElems.map(_._2)),
      "f4 rejections" |: f4results.forall(_.rejections.keySet == Set(t.name)),
      "f1 map" |: {
        val fMap = f1.featureValues[Map[String, Double]]
        val eMap = expected.map(v => (names zip v).toMap)
        // expected map is a superset of feature map
        (fMap zip eMap).forall {
          case (f, e) =>
            f.forall { case (k, v) => e.get(k).map(d2e).contains(d2e(v)) }
        }
      },
      "f2 settings" |: settings == f2.featureSettings,
      "f3 settings" |: settings == f3.featureSettings,
      "f4 settings" |: settings == f4.featureSettings
    )

    val multiSpec = MultiFeatureSpec(fsRequired)

    val flatRequired = FlatExtractor.flatSpec[String, T](fsRequired)
    val flatOptional = FlatExtractor.flatSpec[String, Option[T]](fsOptional)
    val flatMulti = FlatExtractor.multiFlatSpec[String, T](multiSpec)

    val converterRequired = FlatConverter[T, String](fsRequired)
    val converterOptional = FlatConverter[Option[T], String](fsOptional)

    val examplesReq = converterRequired.convert(input)
    val examplesOpt = converterOptional.convert(input.map(Some(_)) :+ None)

    val flatEx = flatRequired.extract(examplesReq)
    val flatExOpt = flatOptional.extract(examplesOpt)

    val flatMultiSettings = flatMulti.extract(examplesReq).featureSettings
    val multiSettings = multiSpec.extract(input).featureSettings

    val flatFeatures = FlatExtractor[List, String](flatEx.featureSettings)
      .featureValues[Seq[Double]](examplesReq)

    val flatFeaturesOpt = FlatExtractor[List, String](flatExOpt.featureSettings)
      .featureValues[Seq[Double]](examplesOpt)

    val propConverters = Prop.all(
      "f1 values flat" |: safeCompare(flatFeatures, expected),
      "f1 values flat opt" |: safeCompare(flatFeaturesOpt, expected :+ missing),
      "Flat Settings Match" |: flatMultiSettings == multiSettings
    )

    Prop.all(propStandard, propConverters)
  }
  // scalastyle:on method.length

  def testException[T](t: Transformer[T, _, _], input: List[T])(p: Throwable => Boolean): Prop =
    try {
      FeatureSpec.of[T].required(identity)(t).extract(input).featureValues[Seq[Double]]
      false
    } catch {
      case e: Exception => p(e)
      case e: Throwable => throw e
    }

  def upperBound(x: Double): Double = if (x > 0.0) x * 2 else x / 2
  def lowerBound(x: Double): Double = if (x < 0.0) x * 2 else x / 2

}
