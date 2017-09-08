package com.spotify.featran

import com.spotify.featran.transformers.Identity
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}

object CrossFeatureSpecSpec extends Properties("FeatureSpec") {
  import FeatureBuilder._

  case class Record(d: Double, optD: Option[Double])

  implicit val arbRecords: Arbitrary[List[Record]] = Arbitrary {
    Gen.listOfN(5, Arbitrary.arbitrary[(Double, Option[Double])].map(Record.tupled))
  }

  private val id = Identity("id")
  private val id2 = Identity("id2")

  property("composite") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record]
      .required(_.d)(Identity("id1"))
      .optional(_.optD, Some(0.5))(Identity("id2"))
      .cross(Cross("id1", "id2", (a, _) => a))
      .extract(xs)

    val features = f.featureValues[Array[Double]].map(_.toList)
    Prop.all(
      f.featureNames == Seq(Seq("id1", "id2", "id1-id2")),
      features == xs.map{r => List(r.d, r.optD.getOrElse(0.5), r.d)}
    )
  }

  property("combine") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].required(_.d)(id)
    val f2 = FeatureSpec.of[Record].required(_.d)(id2)
    val result = FeatureSpec
      .combine(f, f2)
      .cross(Cross("id", "id2", (a, _) => a))
      .extract(xs)

    val features = result.featureResults[Array[Double]]
    Prop.all(
      result.featureNames == Seq(Seq("id", "id2", "id-id2")),
      features.map{r => (r.value.toList, r.original)} == xs.map(r => (List(r.d, r.d, r.d), r)))
  }

}
