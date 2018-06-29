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

import com.spotify.featran.SparseArray
import com.spotify.featran.xgboost.SparseLabeledPoint
import ml.dmlc.xgboost4j.LabeledPoint
import org.scalatest._
import org.tensorflow.example.Example

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class JavaTest extends FlatSpec with Matchers {

  import com.spotify.featran.Fixtures._

  private def e2v(names: Seq[String])(e: Example): Seq[Double] = {
    val m = e.getFeatures.getFeatureMap
    names.map { n =>
      val f = m.get(n)
      if (f == null) 0.0 else f.getFloatList.getValue(0)
    }
  }

  private def l2v(length: Int)(l: SparseLabeledPoint): Seq[Double] =
    SparseArray(l.labeledPoint.indices, l.labeledPoint.values, length).toDense.toSeq.map(_.toDouble)

  "JFeatureSpec" should "work" in {
    val in = TestData.asInstanceOf[Seq[(String, Integer)]]
    val f = JavaTestUtil.spec().extract(in.asJava)
    f.featureNames().asScala shouldBe ExpectedNames
    f.featureValuesFloat().asScala.map(_.toSeq) shouldBe ExpectedValues
    f.featureValuesDouble().asScala.map(_.toSeq) shouldBe ExpectedValues
    f.featureValuesFloatSparse().asScala.map(_.toDense.toSeq) shouldBe ExpectedValues
    f.featureValuesDoubleSparse().asScala.map(_.toDense.toSeq) shouldBe ExpectedValues
    f.featureValuesFloatNamedSparse().asScala.map(_.toDense.toSeq) shouldBe ExpectedValues
    f.featureValuesDoubleNamedSparse().asScala.map(_.toDense.toSeq) shouldBe ExpectedValues
    f.featureValuesExample().asScala.map(e2v(ExpectedNames)) shouldBe ExpectedValues
    f.featureValuesLabeledPoint()
      .asScala
      .map(_.values.toSeq.map(_.toDouble)) shouldBe ExpectedValues
    f.featureValuesSparseLabeledPoint().asScala.map(l2v(ExpectedNames.size)) shouldBe ExpectedValues
  }

  it should "work with Optional" in {
    val in = Seq("a", "b", null)
    val names = Seq("one_hot_a", "one_hot_b")
    val values = Seq(Seq(1.0, 0.0), Seq(0.0, 1.0), Seq(0.0, 0.0))
    val f = JavaTestUtil.optionalSpec().extract(in.asJava)
    f.featureNames().asScala shouldBe names
    f.featureValuesFloat().asScala.map(_.toSeq) shouldBe values
    f.featureValuesDouble().asScala.map(_.toSeq) shouldBe values
    f.featureValuesFloatSparse().asScala.map(_.toDense.toSeq) shouldBe values
    f.featureValuesDoubleSparse().asScala.map(_.toDense.toSeq) shouldBe values
    f.featureValuesFloatNamedSparse().asScala.map(_.toDense.toSeq) shouldBe values
    f.featureValuesDoubleNamedSparse().asScala.map(_.toDense.toSeq) shouldBe values
    f.featureValuesExample().asScala.map(e2v(names)) shouldBe values
    f.featureValuesLabeledPoint().asScala.map(_.values.toSeq.map(_.toDouble)) shouldBe values
    f.featureValuesSparseLabeledPoint().asScala.map(l2v(names.size)) shouldBe values
  }

  it should "work with FeatureSpec" in {
    val f = JFeatureSpec.wrap(TestSpec).extract(TestData.asJava)
    f.featureNames().asScala shouldBe ExpectedNames
    f.featureValuesFloat().asScala.map(_.toSeq) shouldBe ExpectedValues
    f.featureValuesDouble().asScala.map(_.toSeq) shouldBe ExpectedValues
    f.featureValuesFloatSparse().asScala.map(_.toDense.toSeq) shouldBe ExpectedValues
    f.featureValuesDoubleSparse().asScala.map(_.toDense.toSeq) shouldBe ExpectedValues
    f.featureValuesFloatNamedSparse().asScala.map(_.toDense.toSeq) shouldBe ExpectedValues
    f.featureValuesDoubleNamedSparse().asScala.map(_.toDense.toSeq) shouldBe ExpectedValues
    f.featureValuesExample().asScala.map(e2v(ExpectedNames)) shouldBe ExpectedValues
    f.featureValuesLabeledPoint()
      .asScala
      .map(_.values.toSeq.map(_.toDouble)) shouldBe ExpectedValues
    f.featureValuesSparseLabeledPoint().asScala.map(l2v(ExpectedNames.size)) shouldBe ExpectedValues
  }

  it should "work with extractWithSettings" in {
    val fs = JFeatureSpec.wrap(TestSpec)
    val settings = fs.extract(TestData.asJava).featureSettings()
    val n = TestData.size / 2
    val f = fs.extractWithSettings(TestData.take(n).asJava, settings)
    f.featureNames().asScala shouldBe ExpectedNames
    f.featureValuesFloat().asScala.map(_.toSeq) shouldBe ExpectedValues.take(n)
    f.featureValuesDouble().asScala.map(_.toSeq) shouldBe ExpectedValues.take(n)
    f.featureValuesFloatSparse().asScala.map(_.toDense.toSeq) shouldBe ExpectedValues.take(n)
    f.featureValuesDoubleSparse().asScala.map(_.toDense.toSeq) shouldBe ExpectedValues.take(n)
    f.featureValuesFloatNamedSparse().asScala.map(_.toDense.toSeq) shouldBe ExpectedValues.take(n)
    f.featureValuesDoubleNamedSparse().asScala.map(_.toDense.toSeq) shouldBe ExpectedValues.take(n)
    f.featureValuesExample().asScala.map(e2v(ExpectedNames)) shouldBe ExpectedValues.take(n)
    f.featureValuesLabeledPoint()
      .asScala
      .map(_.values.toSeq.map(_.toDouble)) shouldBe ExpectedValues.take(n)
    f.featureValuesSparseLabeledPoint().asScala.map(l2v(ExpectedNames.size)) shouldBe ExpectedValues
      .take(n)
  }

  it should "work with extractWithSettings and RecordExtractor" in {
    val fs = JFeatureSpec.wrap(TestSpec)
    val settings = fs.extract(TestData.asJava).featureSettings()
    val f1 = fs.extractWithSettingsFloat(settings)
    val f2 = fs.extractWithSettingsDouble(settings)
    val f3 = fs.extractWithSettingsFloatSparseArray(settings)
    val f4 = fs.extractWithSettingsDoubleSparseArray(settings)
    val f5 = fs.extractWithSettingsFloatNamedSparseArray(settings)
    val f6 = fs.extractWithSettingsDoubleNamedSparseArray(settings)
    val f7 = fs.extractWithSettingsExample(settings)
    val f8 = fs.extractWithSettingsLabeledPoint(settings)
    val f9 = fs.extractWithSettingsSparseLabeledPoint(settings)
    f1.featureNames().asScala shouldBe ExpectedNames
    f2.featureNames().asScala shouldBe ExpectedNames
    f3.featureNames().asScala shouldBe ExpectedNames
    f4.featureNames().asScala shouldBe ExpectedNames
    f5.featureNames().asScala shouldBe ExpectedNames
    f6.featureNames().asScala shouldBe ExpectedNames
    f7.featureNames().asScala shouldBe ExpectedNames
    f8.featureNames().asScala shouldBe ExpectedNames
    f9.featureNames().asScala shouldBe ExpectedNames
    TestData.map(f1.featureValue).map(_.toSeq) shouldBe ExpectedValues
    TestData.map(f2.featureValue).map(_.toSeq) shouldBe ExpectedValues
    TestData.map(f3.featureValue).map(_.toDense.toSeq) shouldBe ExpectedValues
    TestData.map(f4.featureValue).map(_.toDense.toSeq) shouldBe ExpectedValues
    TestData.map(f5.featureValue).map(_.toDense.toSeq) shouldBe ExpectedValues
    TestData.map(f6.featureValue).map(_.toDense.toSeq) shouldBe ExpectedValues
    TestData.map(f7.featureValue).map(e2v(ExpectedNames)) shouldBe ExpectedValues
    TestData.map(f8.featureValue).map(_.values.toSeq.map(_.toDouble)) shouldBe ExpectedValues
    TestData.map(f9.featureValue).map(l2v(ExpectedNames.size)) shouldBe ExpectedValues
  }

  it should "work with sparse arrays" in {
    val in = Seq("a", "b")
    val indices = Seq(Seq(0), Seq(1))
    val values = Seq(Seq(1.0), Seq(1.0))
    val dense = Seq(Seq(1.0, 0.0), Seq(0.0, 1.0))
    val f = JavaTestUtil.optionalSpec().extract(in.asJava)
    val fs = f.featureValuesFloatSparse().asScala
    fs.map(JavaTestUtil.getIndicies(_).toSeq) shouldBe indices
    fs.map(JavaTestUtil.getValues(_).toSeq) shouldBe values
    fs.map(JavaTestUtil.getDense(_).toSeq) shouldBe dense
    val ds = f.featureValuesDoubleSparse().asScala
    ds.map(JavaTestUtil.getIndicies(_).toSeq) shouldBe indices
    ds.map(JavaTestUtil.getValues(_).toSeq) shouldBe values
    ds.map(JavaTestUtil.getDense(_).toSeq) shouldBe dense
  }

  it should "work with sparse arrays in multithreaded environment" in {
    val in = Seq("a", "b")
    val indices = Seq(Seq(0), Seq(1))
    val values = Seq(Seq(1.0), Seq(1.0))
    val dense = Seq(Seq(1.0, 0.0), Seq(0.0, 1.0))
    import scala.concurrent.ExecutionContext.Implicits.global
    (1 to 5).par
      .map(_ =>
        Future {
          val f = JavaTestUtil.optionalSpec().extract(in.asJava)
          f.featureValuesFloatSparse().asScala
      })
      .map { lfs =>
        val fs = Await.result(lfs, Duration.Inf)
        fs.map(JavaTestUtil.getIndicies(_).toSeq) shouldBe indices
        fs.map(JavaTestUtil.getValues(_).toSeq) shouldBe values
        fs.map(JavaTestUtil.getDense(_).toSeq) shouldBe dense
      }
    (1 to 5).par
      .map(_ =>
        Future {
          val f = JavaTestUtil.optionalSpec().extract(in.asJava)
          f.featureValuesDoubleSparse().asScala
      })
      .map { lfs =>
        val fs = Await.result(lfs, Duration.Inf)
        fs.map(JavaTestUtil.getIndicies(_).toSeq) shouldBe indices
        fs.map(JavaTestUtil.getValues(_).toSeq) shouldBe values
        fs.map(JavaTestUtil.getDense(_).toSeq) shouldBe dense
      }
  }

  it should "work with cross terms" in {
    val data = Seq(("foo", "bar"), ("foo", "baz")).asJava
    val names = Seq("one_hot_a_foo",
                    "one_hot_b_bar",
                    "one_hot_b_baz",
                    "cross_one_hot_a_foo_x_one_hot_b_bar",
                    "cross_one_hot_a_foo_x_one_hot_b_baz")
    val values = Seq(Seq(1.0, 1.0, 0.0, 1.0, 0.0), Seq(1.0, 0.0, 1.0, 0.0, 1.0))
    val fs = JavaTestUtil.crossSpec()
    val f = fs.extract(data)
    f.featureNames().asScala shouldBe names

    val ds = f.featureValuesDouble().asScala
    ds.map(x => x.toSeq) shouldBe values
  }

}
