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

object FeatureSpecSpec extends Properties("FeatureSpec") {

  case class Record(d: Double, optD: Option[Double])

  implicit val arbSeqRecord: Arbitrary[Seq[Record]] = Arbitrary {
    val genRecord = for {
      d <- Arbitrary.arbDouble.arbitrary
      o <- Arbitrary.arbOption[Double].arbitrary
    } yield Record(d, o)
    Gen.listOfN(100, genRecord)
  }

  private val id = Identity("id")

  property("required") = Prop.forAll { xs: Seq[Record] =>
    val f = FeatureSpec.of[Record].required(_.d)(id).extract(xs)
    Prop.all(
      f.featureNames == Seq(Seq("id")),
      f.featureValues[Array[Double]].map(_.toSeq) == xs.map(r => Seq(r.d)))
  }

  property("optional") = Prop.forAll { xs: Seq[Record] =>
    val f = FeatureSpec.of[Record].optional(_.optD)(id).extract(xs)
    Prop.all(
      f.featureNames == Seq(Seq("id")),
      f.featureValues[Array[Double]].map(_.toSeq) == xs.map(r => Seq(r.optD.getOrElse(0.0))))
  }

  property("default") = Prop.forAll { xs: Seq[Record] =>
    val f = FeatureSpec.of[Record].optional(_.optD, Some(0.5))(id).extract(xs)
    Prop.all(
      f.featureNames == Seq(Seq("id")),
      f.featureValues[Array[Double]].map(_.toSeq) == xs.map(r => Seq(r.optD.getOrElse(0.5))))
  }

  property("nones") = Prop.forAll { xs: Seq[Record] =>
    val f = FeatureSpec.of[Record]
      .optional(_.optD, Some(0.5))(id)
      .extract(xs.map(_.copy(optD = None)))
    Prop.all(
      f.featureNames == Seq(Seq("id")),
      f.featureValues[Array[Double]].map(_.toSeq) == xs.map(r => Seq(0.5)))
  }

  property("composite") = Prop.forAll { xs: Seq[Record] =>
    val f = FeatureSpec.of[Record]
      .required(_.d)(Identity("id1"))
      .optional(_.optD, Some(0.5))(Identity("id2"))
      .extract(xs)
    Prop.all(
      f.featureNames == Seq(Seq("id1", "id2")),
      f.featureValues[Array[Double]].map(_.toSeq) == xs.map(r => Seq(r.d, r.optD.getOrElse(0.5))))
  }

}
