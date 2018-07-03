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
  case class RecordWrapper(record: Record, d: Double)

  implicit val arbRecords: Arbitrary[List[Record]] = Arbitrary {
    Gen.listOfN(100, Arbitrary.arbitrary[(Double, Option[Double])].map(Record.tupled))
  }

  implicit val arbWrapperRecords: Arbitrary[List[RecordWrapper]] = Arbitrary {
    Gen.listOfN(100, Arbitrary.arbitrary[(Double, Double, Option[Double])].map {
      case (d1, d2, od) => RecordWrapper(Record(d2, od), d1)
    })
  }

  private val id = Identity("id")
  private val id2 = Identity("id2")
  private val id3 = Identity("id3")

  property("required") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].required(_.d)(id).extract(xs)
    Prop.all(f.featureNames == Seq(Seq("id")),
             f.featureValues[Seq[Double]] == xs.map(r => Seq(r.d)))
  }

  property("optional") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].optional(_.optD)(id).extract(xs)
    Prop.all(f.featureNames == Seq(Seq("id")),
             f.featureValues[Seq[Double]] == xs.map(r => Seq(r.optD.getOrElse(0.0))))
  }

  property("array") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].required(_.d)(id).extract(xs.toArray)
    Prop.all(f.featureNames.toList == Seq(Seq("id")),
             f.featureValues[Seq[Double]].toList == xs.map(r => Seq(r.d)))
  }

  property("default") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].optional(_.optD, Some(0.5))(id).extract(xs)
    Prop.all(f.featureNames == Seq(Seq("id")),
             f.featureValues[Seq[Double]] == xs.map(r => Seq(r.optD.getOrElse(0.5))))
  }

  property("nones") = Prop.forAll { xs: List[Record] =>
    val f =
      FeatureSpec.of[Record].optional(_.optD, Some(0.5))(id).extract(xs.map(_.copy(optD = None)))
    Prop.all(f.featureNames == Seq(Seq("id")),
             f.featureValues[Seq[Double]] == xs.map(r => Seq(0.5)))
  }

  property("generator") = Prop.forAll { xs: List[Record] =>
    val spec = FeatureSpec.from[Record]
    val f = spec.extract(xs)
    Prop.all(f.featureNames == Seq(Seq("d", "optD")), f.featureValues[Seq[Double]] == xs.map { r =>
      r.optD.map(v => Seq(r.d, v)).getOrElse(Seq(r.d, 0.0))
    })
  }

  property("composite") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec
      .of[Record]
      .required(_.d)(Identity("id1"))
      .optional(_.optD, Some(0.5))(Identity("id2"))
      .extract(xs)
    Prop.all(f.featureNames == Seq(Seq("id1", "id2")),
             f.featureValues[Seq[Double]] == xs.map(r => Seq(r.d, r.optD.getOrElse(0.5))))
  }

  property("compose") = Prop.forAll { xs: List[RecordWrapper] =>
    val rSpec = FeatureSpec
      .of[Record]
      .required(_.d)(Identity("id1"))
      .optional(_.optD, Some(0.5))(Identity("id2"))
    val spec = FeatureSpec.of[RecordWrapper].compose(rSpec)(_.record).required(_.d)(Identity("id3"))

    val f = spec.extract(xs)

    Prop.all(f.featureNames == Seq(Seq("id1", "id2", "id3")),
             f.featureValues[Seq[Double]] == xs.map { r =>
               Seq(r.record.d, r.record.optD.getOrElse(0.5), r.d)
             })
  }

  property("original") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].required(_.d)(id).extract(xs)
    Prop.all(f.featureNames == Seq(Seq("id")),
             f.featureResults[Seq[Double]] == xs.map(r => FeatureResult(Seq(r.d), Map.empty, r)))
  }

  property("combine") = Prop.forAll { xs: List[Record] =>
    val f1 = FeatureSpec.of[Record].required(_.d)(id)
    val f2 = FeatureSpec.of[Record].required(_.d)(id2)
    val result = FeatureSpec.combine(f1, f2).extract(xs)
    Prop.all(
      result.featureNames == Seq(Seq("id", "id2")),
      result.featureResults[Seq[Double]] == xs.map(r => FeatureResult(Seq(r.d, r.d), Map.empty, r)))
  }

  property("extra feature in settings") = Prop.forAll { xs: List[Record] =>
    val f1 = FeatureSpec.of[Record].required(_.d)(id).required(_.d)(id2)
    val f2 = FeatureSpec.of[Record].required(_.d)(id)
    val settings = f1.extract(xs).featureSettings
    val f = f2.extractWithSettings(xs, settings)
    Prop.all(f.featureNames == Seq(Seq("id")),
             f.featureResults[Seq[Double]] == xs.map(r => FeatureResult(Seq(r.d), Map.empty, r)))
  }

  property("missing feature in settings") = Prop.forAll { xs: List[Record] =>
    val f1 = FeatureSpec.of[Record].required(_.d)(id)
    val f2 = FeatureSpec.of[Record].required(_.d)(id).required(_.d)(id2)
    val settings = f1.extract(xs).featureSettings
    val t = Try(f2.extractWithSettings(xs, settings).featureValues[Seq[Double]])

    Prop.all(t.isFailure,
             t.failed.get.isInstanceOf[IllegalArgumentException],
             t.failed.get.getMessage == "requirement failed: Missing settings for id2")
  }

  property("record extractor") = Prop.forAll { xs: List[Record] =>
    val f1 = FeatureSpec.of[Record].required(_.d)(id)
    val e1 = f1.extract(xs)
    val settings = e1.featureSettings.head
    val e2 = f1.extractWithSettings[Seq[Double]](settings)

    Prop.all(e1.featureNames.head == e2.featureNames,
             e1.featureValues[Seq[Double]] == xs.map(e2.featureValue),
             e1.featureResults[Seq[Double]] == xs.map(e2.featureResult))
  }

  property("extract specified list of features according to predicate") = Prop.forAll {
    xs: List[Record] =>
      val f = FeatureSpec.of[Record].required(_.d)(id).required(_.d)(id2)
      val includeFeatures = Set(id.name)
      val extracted = f
        .filter { f =>
          includeFeatures.contains(f.transformer.name)
        }
        .extract(xs)
      Prop.all(
        extracted.featureNames.head == Seq("id"),
        extracted.featureResults[Seq[Double]] == xs.map(r => FeatureResult(Seq(r.d), Map.empty, r)))
  }

  property("extract specified list of features according to predicate keeps order") = Prop.forAll {
    xs: List[Record] =>
      val spec = FeatureSpec.of[Record].required(_.d)(id).required(_.d)(id2).required(_.d)(id3)
      val filtered = spec
        .filter { feature =>
          Set(id.name, id3.name).contains(feature.transformer.name)
        }
        .extract(xs)

      val expectedSpec = FeatureSpec.of[Record].required(_.d)(id).required(_.d)(id3)
      val expected = expectedSpec.extract(xs)

      Prop.all(filtered.featureNames == expected.featureNames,
               filtered.featureResults[Seq[Double]] == expected.featureResults[Seq[Double]])
  }

  property("extract with partial settings") = Prop.forAll { xs: List[Record] =>
    val f1 = FeatureSpec.of[Record].required(_.d)(id).required(_.d)(id2).required(_.d)(id3)
    val includeList = Set(id.name, id3.name)
    val e1 = f1
      .filter { f =>
        includeList.contains(f.transformer.name)
      }
      .extract(xs)
    val settings = e1.featureSettings
    val e2 = f1.extractWithSubsetSettings(xs, settings)

    Prop.all(
      e1.featureNames == e2.featureNames,
      e2.featureValues[Seq[Double]] == e1.featureValues[Seq[Double]],
      e2.featureResults[Seq[Double]] == e1.featureResults[Seq[Double]]
    )
  }

  property("record extractor with partial settings") = Prop.forAll { xs: List[Record] =>
    val f1 = FeatureSpec.of[Record].required(_.d)(id).required(_.d)(id2).required(_.d)(id3)
    val includeList = Set(id.name, id3.name)
    val e1 = f1
      .filter { f =>
        includeList.contains(f.transformer.name)
      }
      .extract(xs)
    val settings = e1.featureSettings.head
    val e2 = f1.extractWithSubsetSettings[Seq[Double]](settings)

    Prop.all(e1.featureNames.head == e2.featureNames,
             e1.featureValues[Seq[Double]] == xs.map(e2.featureValue),
             e1.featureResults[Seq[Double]] == xs.map(e2.featureResult))
  }

  property("names") = Prop.forAll(Gen.alphaStr) { s =>
    val msg = if (s == null || s.isEmpty) {
      "requirement failed: name cannot be null or empty"
    } else {
      "requirement failed: duplicate transformer names: " + s
    }
    val t = Try {
      FeatureSpec
        .of[Record]
        .required(_.d)(Identity(s))
        .optional(_.optD)(Identity(s))
        .extract(Seq.empty[Record])
    }
    Prop.all(t.isFailure,
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
      fs.extract(xs.toBuffer).featureValues[Seq[Double]].map(_.head) == xs
    )
  }

}
