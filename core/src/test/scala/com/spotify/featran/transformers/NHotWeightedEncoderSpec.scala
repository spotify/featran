package com.spotify.featran.transformers

import org.scalacheck.{Arbitrary, Gen, Prop}

class NHotWeightedEncoderSpec extends TransformerProp("NHotWeightedEncoder") {
  private implicit val labelArb = Arbitrary {
    Gen.choose(1, 10).flatMap(Gen.listOfN(_, Gen.alphaStr))
  }

  property("default") = Prop.forAll { xs: List[List[String]] =>
    val vecs = xs.map(_.map(n => WeightedValues(n, 0.5)))
    val cats = xs.flatten.distinct.sorted
    val names = cats.map("n_hot_" + _)
    val expected = xs.map(s => cats.map(c => if (s.contains(c)) 0.5 else 0.0))
    val missing = cats.map(_ => 0.0)
    val oob = List((List(WeightedValues("s1", 0.2), WeightedValues("s2", 0.1)), missing))
    test[Seq[WeightedValues]](NHotWeightedEncoder("n_hot"), vecs, names, expected, missing, oob)
  }
}
