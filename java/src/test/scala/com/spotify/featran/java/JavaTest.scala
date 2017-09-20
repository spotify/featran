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

package com.spotify.featran.java

import java.util.Optional

import com.spotify.featran.transformers._
import org.scalatest._

import scala.collection.JavaConverters._

class JavaTest extends FlatSpec with Matchers {

  import com.spotify.featran.Fixtures._

  "JFeatureSpec" should "work" in {
    val f = JFeatureSpec.of[(String, Int)]()
      .required(new SerializableFunction[(String, Int), String] {
        override def apply(input: (String, Int)): String = input._1
      }, OneHotEncoder("one_hot"))
      .required(new SerializableFunction[(String, Int), Double] {
        override def apply(input: (String, Int)): Double = input._2.toDouble
      }, MinMaxScaler("min_max"))
      .extract(testData.asJava)
    f.featureNames().asScala shouldBe expectedNames
    f.featureValuesFloat().asScala.map(_.toSeq) shouldBe expectedValues
    f.featureValuesDouble().asScala.map(_.toSeq) shouldBe expectedValues
    f.featureValuesFloatSparse().asScala.map(_.toDense.toSeq) shouldBe expectedValues
    f.featureValuesDoubleSparse().asScala.map(_.toDense.toSeq) shouldBe expectedValues
  }

  it should "work with Optional" in {
    val in = Seq("a", "b", null)
    val names = Seq("one_hot_a", "one_hot_b")
    val values = Seq(Seq(1.0, 0.0), Seq(0.0, 1.0), Seq(0.0, 0.0))
    val f = JFeatureSpec.of[String]()
      .optional(new SerializableFunction[String, Optional[String]] {
        override def apply(input: String): Optional[String] = Optional.ofNullable(input)
      }, OneHotEncoder("one_hot"))
      .extract(in.asJava)
    f.featureNames().asScala shouldBe names
    f.featureValuesFloat().asScala.map(_.toSeq) shouldBe values
    f.featureValuesDouble().asScala.map(_.toSeq) shouldBe values
    f.featureValuesFloatSparse().asScala.map(_.toDense.toSeq) shouldBe values
    f.featureValuesDoubleSparse().asScala.map(_.toDense.toSeq) shouldBe values
  }

  it should "work with FeatureSpec" in {
    val f = JFeatureSpec.wrap(testSpec).extract(testData.asJava)
    f.featureNames().asScala shouldBe expectedNames
    f.featureValuesFloat().asScala.map(_.toSeq) shouldBe expectedValues
    f.featureValuesDouble().asScala.map(_.toSeq) shouldBe expectedValues
    f.featureValuesFloatSparse().asScala.map(_.toDense.toSeq) shouldBe expectedValues
    f.featureValuesDoubleSparse().asScala.map(_.toDense.toSeq) shouldBe expectedValues
  }

  it should "work with extractWithSettings" in {
    val fs = JFeatureSpec.wrap(testSpec)
    val settings = fs.extract(testData.asJava).featureSettings()
    val n = testData.size / 2
    val f = fs.extractWithSettings(testData.take(n).asJava, settings)
    f.featureNames().asScala shouldBe expectedNames
    f.featureValuesFloat().asScala.map(_.toSeq) shouldBe expectedValues.take(n)
    f.featureValuesDouble().asScala.map(_.toSeq) shouldBe expectedValues.take(n)
    f.featureValuesFloatSparse().asScala.map(_.toDense.toSeq) shouldBe expectedValues.take(n)
    f.featureValuesDoubleSparse().asScala.map(_.toDense.toSeq) shouldBe expectedValues.take(n)
  }

}
