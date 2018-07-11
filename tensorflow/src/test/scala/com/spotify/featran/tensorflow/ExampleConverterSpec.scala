/*
 * Copyright 2018 Spotify AB.
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

import com.spotify.featran.{FeatureSpec, FlatConverter}
import com.spotify.featran.transformers.{MDLRecord, WeightedLabel}
import org.scalacheck._
import com.spotify.featran.transformers._

import scala.collection.JavaConverters._

class ExampleConverterSpec extends Properties("ExampleConverterSpec") {
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

  property("converter") = Prop.forAll { xs: List[Record] =>
    val spec = FeatureSpec.of[Record].required(_.d)(Identity("id"))
    val f = FlatConverter(spec).convert(xs)
    Prop.all(
      f.map(_.getFeatures.getFeatureMap.get("id").getFloatList.getValue(0)) == xs.map(_.d.toFloat)
    )
  }

  property("converter all types") = Prop.forAll { xs: List[TransformerTypes] =>
    val spec = FeatureSpec
      .of[TransformerTypes]
      .required(_.d)(Identity("d"))
      .required(_.s)(OneHotEncoder("s.with$pecial characters"))
      .required(_.ds)(VectorIdentity("ds"))
      .required(_.ss)(NHotEncoder("ss"))
      .required(_.we)(NHotWeightedEncoder("we"))
      .required(_.mdl)(MDL("mdl"))

    val f = FlatConverter(spec).convert(xs)

    val results = f.map { ex =>
      val fm = ex.getFeatures.getFeatureMap.asScala
      TransformerTypes(
        toDoubles(fm("d")).head,
        toStrings(fm("s_with_pecial_characters")).head,
        toDoubles(fm("ds")).toList,
        toStrings(fm("ss")).toList,
        List(WeightedLabel(toStrings(fm("we_key")).head, toDoubles(fm("we_value")).head)),
        MDLRecord(toStrings(fm("mdl_label")).head, toDoubles(fm("mdl_value")).head)
      )
    }

    Prop.all(results == xs)
  }
}
