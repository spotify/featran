package com.spotify.featran

import com.spotify.featran.transformers.Identity
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}

class MultiFeatureSpecSpec extends Properties("MultiFeatureSpec") {
  import FeatureConstructor._

  case class Record(d: Double, optD: Option[Double])

  implicit val arbRecords: Arbitrary[List[Record]] = Arbitrary {
    Gen.listOfN(100, Arbitrary.arbitrary[(Double, Option[Double])].map(Record.tupled))
  }

  private val id = Identity("id")
  private val id2 = Identity("id2")

  property("simple feature extraction") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].required(_.d)(id)
    val f2 = FeatureSpec.of[Record].required(_.d)(id2)

    val multi = MultiFeatureSpec.specs(f, f2).extract(xs)

    Prop.all(
      multi.featureNames == Seq(Seq("id", "id2")),
      multi.featureValues[Seq[Double]] == xs.map(r => Seq(r.d, r.d)))
  }

  property("multi feature extraction") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].required(_.d)(id)
    val f2 = FeatureSpec.of[Record].required(_.d)(id2)

    val multi = MultiFeatureSpec.specs(f, f2).multiExtract(xs)

    Prop.all(
      multi.multiFeatureNames == Seq(Seq(Seq("id"), Seq("id2"))),
      multi.multiFeatureValues[Seq[Double]] == xs.map(r => Seq(Seq(r.d), Seq(r.d)))
    )
  }

  property("multi feature extraction settings") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].required(_.d)(id)
    val f2 = FeatureSpec.of[Record].required(_.d)(id2)

    val settings = MultiFeatureSpec.specs(f, f2).extract(xs).featureSettings

    val multi = MultiFeatureSpec.specs(f, f2).multiExtractWithSettings(xs, settings)

    Prop.all(
      multi.multiFeatureNames == Seq(Seq(Seq("id"), Seq("id2"))),
      multi.multiFeatureValues[Seq[Double]] == xs.map(r => Seq(Seq(r.d), Seq(r.d)))
    )
  }
}
