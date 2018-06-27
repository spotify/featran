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

import breeze.linalg._
import com.spotify.featran.transformers.Identity
import org.scalacheck._

class MultiFeatureSpecSpec extends Properties("MultiFeatureSpec") {

  case class Record(d: Double, optD: Option[Double])

  implicit val arbRecords: Arbitrary[List[Record]] = Arbitrary {
    Gen.listOfN(100, Arbitrary.arbitrary[(Double, Option[Double])].map(Record.tupled))
  }

  private val id = Identity("id")
  private val id2 = Identity("id2")

  private val f = FeatureSpec.of[Record].required(_.d)(id)
  private val f2 = FeatureSpec.of[Record].required(_.d)(id2)

  property("multi feature extraction") = Prop.forAll { xs: List[Record] =>
    val multi = MultiFeatureSpec(f, f2).extract(xs)
    Prop.all(
      multi.featureNames == Seq(Seq(Seq("id"), Seq("id2"))),
      multi.featureValues[Seq[Double]] == xs.map(r => Seq(Seq(r.d), Seq(r.d)))
    )
  }

  property("multi feature extraction based on predicate") = Prop.forAll { xs: List[Record] =>
    val filtered = MultiFeatureSpec(f, f2)
      .filter { feature =>
        Seq(id.name, id2.name).contains(feature.transformer.name)
      }
      .extract(xs)

    val expected = MultiFeatureSpec(f, f2).extract(xs)

    Prop.all(filtered.featureNames == expected.featureNames,
             filtered.featureValues[Seq[Double]] == expected.featureValues[Seq[Double]])
  }

  property("multi feature extraction based on predicate keeps order") = Prop.forAll {
    xs: List[Record] =>
      val multi = MultiFeatureSpec(f, f2)
        .filter { feature =>
          feature.transformer.name == id.name
        }
        .extract(xs)

      Prop.all(
        multi.featureNames == Seq(Seq(Seq(id.name))),
        multi.featureValues[Seq[Double]] == xs.map(r => Seq(Seq(r.d)))
      )
  }

  property("multi feature extraction based on partial settings") = Prop.forAll { xs: List[Record] =>
    val settings: Seq[String] = MultiFeatureSpec(f, f2)
      .filter { feature =>
        feature.transformer.name == id.name
      }
      .extract(xs)
      .featureSettings

    val multi = MultiFeatureSpec(f, f2).extractWithSubsetSettings(xs, settings)
    Prop.all(
      multi.featureNames == Seq(Seq(Seq(id.name))),
      multi.featureValues[Seq[Double]] == xs.map(r => Seq(Seq(r.d)))
    )
  }

  property("multi feature extraction map") = Prop.forAll { xs: List[Record] =>
    val multi = MultiFeatureSpec(f, f2).extract(xs)
    val expected = xs.map(r => Seq(Map("id" -> r.d), Map("id2" -> r.d)))
    Prop.all(multi.featureNames == Seq(Seq(Seq("id"), Seq("id2"))),
             multi.featureValues[Map[String, Double]] == expected)
  }

  property("multi feature extraction sparse") = Prop.forAll { xs: List[Record] =>
    val multi = MultiFeatureSpec(f, f2).extract(xs)
    val expected =
      xs.map(r => Seq(SparseVector(1)((0, r.d)), SparseVector(1)((0, r.d))))
    Prop.all(
      multi.featureNames == Seq(Seq(Seq("id"), Seq("id2"))),
      multi.featureValues[SparseVector[Double]] == expected
    )
  }

  property("multi feature extraction dense") = Prop.forAll { xs: List[Record] =>
    val multi = MultiFeatureSpec(f, f2).extract(xs)
    val expected = xs.map(r => Seq(DenseVector(r.d), DenseVector(r.d)))
    Prop.all(
      multi.featureNames == Seq(Seq(Seq("id"), Seq("id2"))),
      multi.featureValues[DenseVector[Double]] == expected
    )
  }

  property("multi feature extraction settings") = Prop.forAll { xs: List[Record] =>
    val settings = MultiFeatureSpec(f, f2).extract(xs).featureSettings
    val multi = MultiFeatureSpec(f, f2).extractWithSettings(xs, settings)
    Prop.all(
      multi.featureNames == Seq(Seq(Seq("id"), Seq("id2"))),
      multi.featureValues[Seq[Double]] == xs.map(r => Seq(Seq(r.d), Seq(r.d)))
    )
  }

}
