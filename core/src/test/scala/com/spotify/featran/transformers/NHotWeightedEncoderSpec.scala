package com.spotify.featran.transformers

import org.scalacheck.{Arbitrary, Gen, Prop}

class NHotWeightedEncoderSpec extends TransformerProp("NHotWeightedEncoder") {
  private implicit val weightedVector = Arbitrary {
    val weightedValueGen = for {
      value <- Gen.chooseNum(-1.0, 1.0)
      n <- Arbitrary.arbString.arbitrary
    } yield WeightedValue(n, value)

    Gen.choose(1, 5).flatMap(Gen.listOfN(_, weightedValueGen))
  }

  property("default") = Prop.forAll { xs: List[List[WeightedValue]] =>
    val cats = xs.flatten.map(_.name).distinct.sorted
    val names = cats.map("n_hot_" + _)
    val expected = xs.map(s => cats.map(c => s.find(_.name == c).map(_.value).getOrElse(0.0)))
    val missing = cats.map(_ => 0.0)
    val oob = List((List(WeightedValue("s1", 0.2), WeightedValue("s2", 0.1)), missing))
    test[Seq[WeightedValue]](NHotWeightedEncoder("n_hot"), xs, names, expected, missing, oob)
  }
}
