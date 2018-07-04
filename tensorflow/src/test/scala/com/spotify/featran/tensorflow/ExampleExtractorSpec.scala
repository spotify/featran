/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.featran.tensorflow

import com.spotify.featran.transformers._
import com.spotify.featran.{FeatureSpec, FlatExtractor}
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}
import org.tensorflow.example.{Example, Features}
import shapeless.datatype.tensorflow._

import scala.reflect.ClassTag

case class TFRecord(d: Float, optD: Option[Float])

case class TFStr(d: String)

object ExampleExtractorSpec extends Properties("ExampleExtractorSpec") {
  import TensorFlowType._

  implicit val arbRecords: Arbitrary[List[TFRecord]] = Arbitrary {
    Gen.listOfN(100, Arbitrary.arbitrary[(Float, Option[Float])].map(TFRecord.tupled))
  }

  implicit val arbStrs: Arbitrary[List[TFStr]] = Arbitrary {
    Gen.listOfN(100, Arbitrary.arbitrary[(String)].map(v => TFStr(v)))
  }

  def toExample(features: Features): Example =
    Example
      .newBuilder()
      .setFeatures(features)
      .build()

  def toExample(r: TFRecord): Example = {
    val features = Features.newBuilder()

    features.putFeature("d", fromFloats(Seq(r.d)).build())
    if (r.optD.isDefined) {
      features.putFeature("optD", fromFloats(r.optD.toList).build())
    }

    toExample(features.build())
  }

  private def convertTest[T: ClassTag, A](
    fn: T => A,
    ex: T => Example
  )(recs: List[T], t: Transformer[A, _, _]) = {
    val spec = FeatureSpec
      .of[T]
      .required(r => fn(r))(t)

    val f = spec.extract(recs)
    val asExamples = recs.map(r => ex(r))

    val flatSpec = FlatExtractor.flatSpec[Example, T](spec)
    val flatEx = flatSpec.extract(asExamples)

    val normalSettings = f.featureSettings
    val flatSettings = flatEx.featureSettings

    val exTransformer = FlatExtractor[List, Example](normalSettings)
    val featranValues = f.featureValues[Seq[Double]]
    val flatValues = flatEx.featureValues[Seq[Double]]
    val exValues = exTransformer.featureValues[Seq[Double]](asExamples)

    val propSetting = Prop.all(featranValues == exValues)
    val propFlatSpec = Prop.all(flatValues == exValues)
    val propSettingFile = Prop.all(normalSettings == flatSettings)

    Prop.all(propSetting, propFlatSpec, propSettingFile)
  }

  def exDouble(r: TFRecord): Example = {
    val features = Features.newBuilder()
    features.putFeature("d", fromFloats(Seq(r.d)).build())
    toExample(features.build())
  }

  def exStr(r: TFStr): Example = {
    val features = Features.newBuilder()
    features.putFeature("d", fromStrings(Seq(r.d)).build())
    toExample(features.build())
  }

  private def numeric = {
    val fn = (r: TFRecord) => r.d.toDouble
    convertTest(fn, exDouble) _
  }

  property("continuous") = Prop.forAll { xs: List[TFRecord] =>
    val transformers: List[Transformer[Double, _, _]] = List(
      Identity("d"),
      MinMaxScaler("d"),
      Binarizer("d"),
      MaxAbsScaler("d"),
      StandardScaler("d")
    )

    val props = transformers.map(t => numeric(xs, t))
    Prop.all(props: _*)
  }

  private def vector = {
    val fn = (r: TFRecord) => Array(r.d.toDouble)
    convertTest(fn, exDouble) _
  }

  private def vectorSeq = {
    val fn = (r: TFRecord) => List(r.d.toDouble)
    convertTest(fn, exDouble) _
  }

  property("vector") = Prop.forAll { xs: List[TFRecord] =>
    val transformersSeq: List[Transformer[Seq[Double], _, _]] = List(
      VectorIdentity[Seq]("d")
    )

    val transformers: List[Transformer[Array[Double], _, _]] = List(
      Normalizer("d"),
      PolynomialExpansion("d")
    )

    val seqProps = transformersSeq.map(t => vectorSeq(xs, t))
    val props = transformers.map(t => vector(xs, t))

    Prop.all((props ++ seqProps): _*)
  }

  private def str = {
    val fn = (r: TFStr) => r.d
    convertTest(fn, exStr) _
  }

  property("enums") = Prop.forAll { xs: List[TFStr] =>
    val transformers: List[Transformer[String, _, _]] = List(
      OneHotEncoder("d"),
      HashOneHotEncoder("d"),
      PositionEncoder("d"),
      HeavyHitters("d", 2)
    )

    val props = transformers.map(t => str(xs, t))
    Prop.all(props: _*)
  }

  private def strs = {
    val fn = (r: TFStr) => Seq(r.d)
    convertTest(fn, exStr) _
  }

  property("n-enums") = Prop.forAll { xs: List[TFStr] =>
    val transformers: List[Transformer[Seq[String], _, _]] = List(
      NHotEncoder("d"),
      HashNHotEncoder("d"),
      NGrams("d")
    )

    val props = transformers.map(t => strs(xs, t))
    Prop.all(props: _*)
  }

  private def weighted = {
    val cv = (r: TFStr) => {
      val features = Features.newBuilder()
      features.putFeature("d_key", fromStrings(Seq(r.d)).build())
      features.putFeature("d_value", fromDoubles(Seq(1.0)).build())
      toExample(features.build())
    }

    val fn = (r: TFStr) => List(WeightedLabel(r.d, 1.0))

    convertTest(fn, cv) _
  }

  property("weighted") = Prop.forAll { xs: List[TFStr] =>
    val transformers: List[Transformer[Seq[WeightedLabel], _, _]] = List(
      NHotWeightedEncoder("d"),
      HashNHotWeightedEncoder("d")
    )

    val props = transformers.map(t => weighted(xs, t))
    Prop.all(props: _*)
  }
}
