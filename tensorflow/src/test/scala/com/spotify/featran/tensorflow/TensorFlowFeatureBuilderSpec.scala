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

package com.spotify.featran.tensorflow

import com.spotify.featran.transformers._
import com.spotify.featran.{FeatureBuilder, FeatureSpec, SerializableUtils}
import org.scalacheck._
import org.tensorflow.example.{Example, Feature, Features, FloatList}

import scala.collection.JavaConverters._

object TensorFlowFeatureBuilderSpec extends Properties("TensorFlowFeatureBuilder") {
  import shapeless.datatype.tensorflow.TensorFlowType._
  case class Record(d: Double, optD: Option[Double])
  case class TransformerTypes(
    d: Double,
    s: String,
    ds: List[Double],
    ss: List[String],
    we: List[WeightedLabel],
    mdl: MDLRecord[String]
  )

  private def list[T](implicit arb: Arbitrary[Option[T]]): Gen[List[Option[T]]] =
    Gen.listOfN(100, arb.arbitrary)

  implicit val arbRecords: Arbitrary[List[Record]] = Arbitrary {
    Gen.listOfN(100, Arbitrary.arbitrary[(Double, Option[Double])].map(Record.tupled))
  }

  implicit val arbTypes: Arbitrary[List[TransformerTypes]] = Arbitrary {
    Gen.listOfN(
      100,
      Arbitrary
        .arbitrary[(Float, String)]
        .map {
          case (num, str) =>
            TransformerTypes(
              num.toDouble,
              str,
              List(num.toDouble),
              List(str),
              List(WeightedLabel(str, num.toDouble)),
              MDLRecord(str, num.toDouble)
            )
        }
    )
  }

  property("TensorFlow Example") = Prop.forAll(list[Double]) { xs =>
    val fb = SerializableUtils.ensureSerializable(FeatureBuilder[Example])
    fb.init(xs.size + 4)
    val b = Features.newBuilder()
    xs.zipWithIndex.foreach {
      case (Some(x), i) =>
        val key = "key" + i.toString
        fb.add(key, x)
        b.putFeature(
          key,
          Feature.newBuilder().setFloatList(FloatList.newBuilder().addValue(x.toFloat)).build())
      case (None, _) => fb.skip()
    }
    fb.add(Iterable("x", "y"), Seq(0.0, 0.0))
    fb.skip(2)
    val actual = fb.result
    b.putFeature("x",
                 Feature.newBuilder().setFloatList(FloatList.newBuilder().addValue(0.0f)).build())
    b.putFeature("y",
                 Feature.newBuilder().setFloatList(FloatList.newBuilder().addValue(0.0f)).build())
    val expected = Example.newBuilder().setFeatures(b).build()
    actual == expected
  }

  property("converter") = Prop.forAll { xs: List[Record] =>
    val f = FeatureSpec.of[Record].required(_.d)(Identity("id")).convert(xs)
    Prop.all(
      f.map(_.getFeatures.getFeatureMap.get("id").getFloatList.getValue(0)) == xs.map(_.d.toFloat)
    )
  }

  property("feature names") = Prop.forAll { key: String =>
    val fb = SerializableUtils.ensureSerializable(FeatureBuilder[Example])
    fb.init(1)
    fb.add(key, 0.0)
    val actual = fb.result.getFeatures.getFeatureMap.keySet().iterator().next()
    Prop.all(actual.length == key.length, actual.replaceAll("[^A-Za-z0-9_]", "_") == actual)
  }

  property("converter all types") = Prop.forAll { xs: List[TransformerTypes] =>
    val f = FeatureSpec
      .of[TransformerTypes]
      .required(_.d)(Identity("d"))
      .required(_.s)(OneHotEncoder("s"))
      .required(_.ds)(VectorIdentity("ds"))
      .required(_.ss)(NHotEncoder("ss"))
      .required(_.we)(NHotWeightedEncoder("we"))
      .required(_.mdl)(MDL("mdl"))
      .convert(xs)

    val results = f.map { ex =>
      val fm = ex.getFeatures.getFeatureMap.asScala
      TransformerTypes(
        toDoubles(fm("d")).head,
        toStrings(fm("s")).head,
        toDoubles(fm("ds")).toList,
        toStrings(fm("ss")).toList,
        List(WeightedLabel(toStrings(fm("we_key")).head, toDoubles(fm("we_value")).head)),
        MDLRecord(toStrings(fm("mdl_label")).head, toDoubles(fm("mdl_value")).head)
      )
    }

    Prop.all(results == xs)
  }
}
