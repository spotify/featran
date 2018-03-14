package com.spotify.featran.transformers

import org.scalacheck.{Arbitrary, Gen, Prop}

class PositionEncoderSpec extends TransformerProp("PositionEncoder") {

  private implicit val labelArb = Arbitrary(Gen.alphaStr)

  property("default") = Prop.forAll { xs: List[String] =>
    val cats = xs.distinct.sorted
    val expected =
      xs.map(s => Seq(cats.zipWithIndex.find(c => s == c._1).map(_._2).getOrElse(0).toDouble))
    val oob = List(("s1", Seq(0.0)), ("s2", Seq(0.0))) // unseen labels
    test(PositionEncoder("position"), xs, List("position"), expected, Seq(0.0), oob)
  }

}
