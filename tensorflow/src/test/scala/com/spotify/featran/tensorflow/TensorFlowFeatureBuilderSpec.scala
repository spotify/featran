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

import com.spotify.featran.{FeatureBuilder, SerializableUtils}
import org.scalacheck._
import org.tensorflow.example.{Example, Feature, Features, FloatList}

object TensorFlowFeatureBuilderSpec extends Properties("TensorFlowFeatureBuilder") {

  private def list[T](implicit arb: Arbitrary[Option[T]]): Gen[List[Option[T]]] =
    Gen.listOfN(100, arb.arbitrary)

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

  property("feature names") = Prop.forAll { key: String =>
    val fb = SerializableUtils.ensureSerializable(FeatureBuilder[Example])
    fb.init(1)
    fb.add(key, 0.0)
    val actual = fb.result.getFeatures.getFeatureMap.keySet().iterator().next()
    Prop.all(actual.length == key.length, actual.replaceAll("[^A-Za-z0-9_]", "_") == actual)
  }

}
