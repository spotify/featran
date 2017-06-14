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
import org.scalacheck._

import scala.util.Try

object FeatureSpecSpec extends Properties("FeatureSpec") {

  case class Record(d: Double, optD: Option[Double])

  implicit val arbRecords: Arbitrary[List[Record]] = Arbitrary {
    Gen.listOfN(100, Arbitrary.arbitrary[(Double, Option[Double])].map(Record.tupled))
  }

  private val id = Identity("id")
  private val id2 = Identity("id2")

  property("required") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].required(_.d)(id).extract(xs)
    Prop.all(
      f.featureNames == Seq(Seq("id")),
      f.featureValues[Seq[Double]] == xs.map(r => Seq(r.d)))
  }

  property("optional") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].optional(_.optD)(id).extract(xs)
    Prop.all(
      f.featureNames == Seq(Seq("id")),
      f.featureValues[Seq[Double]] == xs.map(r => Seq(r.optD.getOrElse(0.0))))
  }

  property("array") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].required(_.d)(id).extract(xs.toArray)
    Prop.all(
      f.featureNames.toList == Seq(Seq("id")),
      f.featureValues[Seq[Double]].toList == xs.map(r => Seq(r.d)))
  }

  property("default") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].optional(_.optD, Some(0.5))(id).extract(xs)
    Prop.all(
      f.featureNames == Seq(Seq("id")),
      f.featureValues[Seq[Double]] == xs.map(r => Seq(r.optD.getOrElse(0.5))))
  }

  property("nones") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record]
      .optional(_.optD, Some(0.5))(id)
      .extract(xs.map(_.copy(optD = None)))
    Prop.all(
      f.featureNames == Seq(Seq("id")),
      f.featureValues[Seq[Double]] == xs.map(r => Seq(0.5)))
  }

  property("composite") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record]
      .required(_.d)(Identity("id1"))
      .optional(_.optD, Some(0.5))(Identity("id2"))
      .extract(xs)
    Prop.all(
      f.featureNames == Seq(Seq("id1", "id2")),
      f.featureValues[Seq[Double]] == xs.map(r => Seq(r.d, r.optD.getOrElse(0.5))))
  }

  property("original") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].required(_.d)(id).extract(xs)
    Prop.all(
      f.featureNames == Seq(Seq("id")),
      f.featureValuesWithOriginal[Seq[Double]] == xs.map(r => (Seq(r.d), r)))
  }

  property("extra feature in settings") = Prop.forAll { xs: List[Record] =>
    val spec1 = FeatureSpec.of[Record].required(_.d)(id).required(_.d)(id2)
    val spec2 = FeatureSpec.of[Record].required(_.d)(id)
    val settings = spec1.extract(xs).featureSettings
    val f = spec2.extractWithSettings(xs,  settings)

    Prop.all(
      f.featureNames == Seq(Seq("id")),
      f.featureValuesWithOriginal[Seq[Double]] == xs.map(r => (Seq(r.d), r)))
  }

  property("missing feature in settings") = Prop.forAll { xs: List[Record] =>
    val spec1 = FeatureSpec.of[Record].required(_.d)(id)
    val spec2 = FeatureSpec.of[Record].required(_.d)(id).required(_.d)(id2)
    val settings = spec1.extract(xs).featureSettings
    val t = Try(spec2.extractWithSettings(xs,  settings).featureValues[Seq[Double]])

    Prop.all(
      t.isFailure,
      t.failed.get.isInstanceOf[IllegalArgumentException],
      t.failed.get.getMessage == "requirement failed: Missing settings for id2")
  }

  property("names") = Prop.forAll(Gen.alphaStr) { s =>
    val msg = if (s == null || s.isEmpty) {
      "requirement failed: name cannot be null or empty"
    } else {
      "requirement failed: duplicate transformer names: " + s
    }
    val t = Try {
      FeatureSpec.of[Record]
        .required(_.d)(Identity(s))
        .optional(_.optD)(Identity(s))
        .extract(Seq.empty[Record])
    }
    Prop.all(
      t.isFailure,
      t.failed.get.isInstanceOf[IllegalArgumentException],
      t.failed.get.getMessage == msg)
  }

  property("seq") = Prop.forAll(Gen.listOfN(100, Arbitrary.arbitrary[Double])) { xs =>
    val fs = FeatureSpec.of[Double].required(identity)(Identity("id"))
    Prop.all(
      fs.extract(xs.asInstanceOf[Traversable[Double]]).featureValues[Seq[Double]].map(_.head) == xs,
      fs.extract(xs.asInstanceOf[Iterable[Double]]).featureValues[Seq[Double]].map(_.head) == xs,
      fs.extract(xs.asInstanceOf[Seq[Double]]).featureValues[Seq[Double]].map(_.head) == xs,
      fs.extract(xs.toIndexedSeq).featureValues[Seq[Double]].map(_.head) == xs,
      fs.extract(xs).featureValues[Seq[Double]].map(_.head) == xs, // List
      fs.extract(xs.toVector).featureValues[Seq[Double]].map(_.head) == xs.toVector,
      fs.extract(xs.toBuffer).featureValues[Seq[Double]].map(_.head) == xs)
  }

}
