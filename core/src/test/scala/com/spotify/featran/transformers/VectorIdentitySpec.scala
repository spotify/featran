package com.spotify.featran.transformers

import org.scalacheck.Prop

class VectorIdentitySpec extends TransformerProp("VectorIdentity") {
  property("default") = Prop.forAll { xs: List[Double] =>
    val vecs = xs.map(a => Seq(a))
    test[Seq[Double]](VectorIdentity("id", 1), vecs, Seq("id_0"), xs.map(Seq(_)), Seq(0.0))
  }
}
