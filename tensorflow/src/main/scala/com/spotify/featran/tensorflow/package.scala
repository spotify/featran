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

package com.spotify.featran

import org.tensorflow.example.{Example, Features}
import org.tensorflow.{example => tf}
import _root_.java.util.regex.Pattern

import com.spotify.featran.transformers.{MDLRecord, WeightedLabel}
import shapeless.datatype.tensorflow.TensorFlowType

package object tensorflow {
  private val FeatureNameNormalization = Pattern.compile("[^A-Za-z0-9_]")

  final case class TensorFlowFeatureBuilder(
    @transient private var underlying: Features.Builder = tf.Features.newBuilder())
      extends FeatureBuilder[tf.Example] {
    override def init(dimension: Int): Unit = {
      if (underlying == null) {
        underlying = tf.Features.newBuilder()
      }
      underlying.clear()
    }
    override def add(name: String, value: Double): Unit = {
      val feature = tf.Feature
        .newBuilder()
        .setFloatList(tf.FloatList.newBuilder().addValue(value.toFloat))
        .build()
      val normalized = FeatureNameNormalization.matcher(name).replaceAll("_")
      underlying.putFeature(normalized, feature)
    }
    override def skip(): Unit = Unit
    override def skip(n: Int): Unit = Unit
    override def result: tf.Example =
      tf.Example.newBuilder().setFeatures(underlying).build()

    override def newBuilder: FeatureBuilder[Example] = TensorFlowFeatureBuilder()
  }

  implicit val exampleFlatReader: FlatReader[Example] = new FlatReader[tf.Example] {
    import TensorFlowType._

    def toFeature(name: String, ex: Example): Option[tf.Feature] = {
      val fm = ex.getFeatures.getFeatureMap
      if (fm.containsKey(name)) {
        Some(fm.get(name))
      } else {
        None
      }
    }

    def getDouble(name: String): Example => Option[Double] =
      (ex: Example) => toFeature(name, ex).flatMap(v => toDoubles(v).headOption)

    def getMdlRecord(name: String): Example => Option[MDLRecord[String]] =
      (ex: Example) => {
        for {
          labelFeature <- toFeature(name + "_label", ex)
          label <- toStrings(labelFeature).headOption
          valueFeature <- toFeature(name + "_value", ex)
          value <- toDoubles(valueFeature).headOption
        } yield MDLRecord(label, value)
      }

    def getWeightedLabel(name: String): Example => Option[List[WeightedLabel]] =
      (ex: Example) => {
        val labels = for {
          keyFeature <- toFeature(name + "_key", ex).toList
          key <- toStrings(keyFeature)
          valueFeature <- toFeature(name + "_value", ex).toList
          value <- toDoubles(valueFeature)
        } yield WeightedLabel(key, value)
        if (labels.isEmpty) None else Some(labels)
      }

    def getDoubles(name: String): Example => Option[Seq[Double]] =
      (ex: Example) => toFeature(name, ex).map(v => toDoubles(v))

    def getDoubleArray(name: String): Example => Option[Array[Double]] =
      (ex: Example) => toFeature(name, ex).map(v => toDoubles(v).toArray)

    def getString(name: String): Example => Option[String] =
      (ex: Example) => toFeature(name, ex).flatMap(v => toStrings(v).headOption)

    def getStrings(name: String): Example => Option[Seq[String]] =
      (ex: Example) => toFeature(name, ex).map(v => toStrings(v))
  }

  /**
   * [[FeatureBuilder]] for output as TensorFlow `Example` type.
   */
  implicit def tensorFlowFeatureBuilder: FeatureBuilder[tf.Example] = TensorFlowFeatureBuilder()
}
