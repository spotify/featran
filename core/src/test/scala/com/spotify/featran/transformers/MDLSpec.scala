package com.spotify.featran.transformers

import com.spotify.featran.transformers.mdl.MDLPDiscretizer
import org.scalacheck._

class MDLSpec extends TransformerProp("MDL") {

  private implicit val arbPosDouble = Arbitrary(Gen.posNum[Double])

  private implicit val mdlRecords = Arbitrary {
    for {
      value <- Gen.oneOf(Seq("1", "2", "3"))
      n <- Gen.posNum[Double]
    } yield MDLRecord(value, n)
  }

  property("default") = Prop.forAll { xs: List[MDLRecord[String]] =>
    val ranges = new MDLPDiscretizer[String](xs.map(l => (l.label, l.value))).discretize()

    val slices = ranges.tail
    val names = slices.indices.map(idx => s"mdl_$idx")

    val expected = xs.map{case MDLRecord(_, x) =>
      val array = Array.fill[Double](slices.size)(0.0)
      val bin = slices
        .zipWithIndex
        .find(_._1.toDouble > x)
        .map(_._2)
        .getOrElse(slices.length-1)

      array(bin) = 1.0
      array.toList
    }

    val missing = slices.indices.map(_ => 0.0)
    test[MDLRecord[String]](MDL[String]("mdl"), xs, names, expected, missing)
  }
}
