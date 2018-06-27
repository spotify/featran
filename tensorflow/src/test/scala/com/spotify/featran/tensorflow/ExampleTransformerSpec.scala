package com.spotify.featran.tensorflow

import com.spotify.featran.FeatureSpec
import com.spotify.featran.transformers.Identity
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}
import org.tensorflow.example.{Example, Features}
import shapeless.datatype.tensorflow._

case class TFRecord(d: Float, optD: Option[Float])

class ExampleTransformerSpec extends Properties("FeatureSpec") {
  import TensorFlowType._

  implicit val arbRecords: Arbitrary[List[TFRecord]] = Arbitrary {
    Gen.listOfN(100, Arbitrary.arbitrary[(Float, Option[Float])].map(TFRecord.tupled))
  }

  def toExample(r: TFRecord): Example = {
    val features = Features.newBuilder()

    features.putFeature("d", fromFloats(Seq(r.d)).build())
    if (r.optD.isDefined) {
      features.putFeature("optD", fromFloats(r.optD.toList).build())
    }
    Example
      .newBuilder()
      .setFeatures(features)
      .build()
  }

  property("transform") = Prop.forAll { xs: List[TFRecord] =>
    val f = FeatureSpec
      .of[TFRecord]
      .required(_.d.toDouble)(Identity("d"))
      .optional(_.optD.map(_.toDouble))(Identity("optD"))
      .extract(xs)

    val exTransformer = ExampleTransformer(f.featureSettings)

    val featranValues = f.featureValues[Seq[Double]]

    val asExamples = xs.map(toExample)

    val exValues = exTransformer.transform[Seq[Double]](asExamples)

    Prop.all(featranValues == exValues)
  }
}
