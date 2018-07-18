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

import _root_.java.util.regex.Pattern
import _root_.java.util.concurrent.ConcurrentHashMap
import _root_.java.util.function.Function

import com.spotify.featran.transformers.{MDLRecord, WeightedLabel}
import org.tensorflow.example.{Example, Features}
import org.tensorflow.{example => tf}
import shapeless.datatype.tensorflow.TensorFlowType

package object tensorflow {

  private[this] object FeatureNameNormalization {
    private[this] val NamePattern = Pattern.compile("[^A-Za-z0-9_]")

    val normalize: String => String = {
      lazy val cache = new ConcurrentHashMap[String, String]()
      fn =>
        cache.computeIfAbsent(fn, new Function[String, String] {
          override def apply(n: String): String =
            NamePattern.matcher(n).replaceAll("_")
        })
    }
  }

  final case class NamedTFFeature(name: String, f: tf.Feature)

  final case class TensorFlowFeatureBuilder(
    @transient private var underlying: tf.Features.Builder = tf.Features.newBuilder())
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
      val normalized = FeatureNameNormalization.normalize(name)
      underlying.putFeature(normalized, feature)
    }
    override def skip(): Unit = Unit
    override def skip(n: Int): Unit = Unit
    override def result: tf.Example =
      tf.Example.newBuilder().setFeatures(underlying).build()

    override def newBuilder: FeatureBuilder[tf.Example] = TensorFlowFeatureBuilder()
  }

  /**
   * [[FeatureBuilder]] for output as TensorFlow `Example` type.
   */
  implicit def tensorFlowFeatureBuilder: FeatureBuilder[tf.Example] = TensorFlowFeatureBuilder()

  implicit val exampleFlatReader: FlatReader[tf.Example] = new FlatReader[tf.Example] {
    import TensorFlowType._

    override type ReadType = String => Option[tf.Feature]

    def readDouble(name: String): ReadType => Option[Double] =
      rt => rt(name).flatMap(v => toDoubles(v).headOption)

    def readMdlRecord(name: String): ReadType => Option[MDLRecord[String]] =
      rt => {
        for {
          labelFeature <- rt(name + "_label")
          label <- toStrings(labelFeature).headOption
          valueFeature <- rt(name + "_value")
          value <- toDoubles(valueFeature).headOption
        } yield MDLRecord(label, value)
      }

    def readWeightedLabel(name: String): ReadType => Option[List[WeightedLabel]] =
      rt => {
        val labels = for {
          keyFeature <- rt(name + "_key").toList
          key <- toStrings(keyFeature)
          valueFeature <- rt(name + "_value").toList
          value <- toDoubles(valueFeature)
        } yield WeightedLabel(key, value)
        if (labels.isEmpty) None else Some(labels)
      }

    def readDoubles(name: String): ReadType => Option[Seq[Double]] =
      rt => rt(name).map(v => toDoubles(v))

    def readDoubleArray(name: String): ReadType => Option[Array[Double]] =
      rt => rt(name).map(v => toDoubles(v).toArray)

    def readString(name: String): ReadType => Option[String] =
      rt => rt(name).flatMap(v => toStrings(v).headOption)

    def readStrings(name: String): ReadType => Option[Seq[String]] =
      rt => rt(name).map(v => toStrings(v))

    override def reader: Example => String => Option[tf.Feature] = ex => {
      val fm = ex.getFeatures.getFeatureMap
      name =>
        if (fm.containsKey(name)) {
          Some(fm.get(name))
        } else {
          None
        }
    }
  }

  implicit val exampleFlatWriter: FlatWriter[Example] = new FlatWriter[tf.Example] {
    import TensorFlowType._
    type IF = List[NamedTFFeature]

    override def writeDouble(name: String): Option[Double] => List[NamedTFFeature] =
      (v: Option[Double]) => v.toList.map(r => NamedTFFeature(name, fromDoubles(Seq(r)).build()))

    override def writeMdlRecord(name: String): Option[MDLRecord[String]] => List[NamedTFFeature] =
      (v: Option[MDLRecord[String]]) => {
        v.toList.flatMap { values =>
          List(
            NamedTFFeature(name + "_label", fromStrings(Seq(values.label.toString)).build()),
            NamedTFFeature(name + "_value", fromDoubles(Seq(values.value)).build())
          )
        }
      }

    override def writeWeightedLabel(n: String): Option[Seq[WeightedLabel]] => List[NamedTFFeature] =
      (v: Option[Seq[WeightedLabel]]) => {
        v.toList.flatMap { values =>
          List(
            NamedTFFeature(n + "_key", fromStrings(values.map(_.name)).build()),
            NamedTFFeature(n + "_value", fromDoubles(values.map(_.value)).build())
          )
        }
      }

    override def writeDoubles(name: String): Option[Seq[Double]] => List[NamedTFFeature] =
      (v: Option[Seq[Double]]) => {
        v.toList.flatMap { values =>
          List(NamedTFFeature(name, fromDoubles(values).build()))
        }
      }

    override def writeDoubleArray(name: String): Option[Array[Double]] => List[NamedTFFeature] =
      (v: Option[Array[Double]]) => {
        v.toList.flatMap { values =>
          List(NamedTFFeature(name, fromDoubles(values).build()))
        }
      }

    override def writeString(name: String): Option[String] => List[NamedTFFeature] =
      (v: Option[String]) => {
        v.toList.flatMap { values =>
          List(NamedTFFeature(name, fromStrings(Seq(values)).build()))
        }
      }

    override def writeStrings(name: String): Option[Seq[String]] => List[NamedTFFeature] =
      (v: Option[Seq[String]]) => {
        v.toList.flatMap { values =>
          List(NamedTFFeature(name, fromStrings(values).build()))
        }
      }

    override def writer: Seq[List[NamedTFFeature]] => Example =
      (fns: Seq[List[NamedTFFeature]]) => {
        val builder = Features.newBuilder()
        fns.foreach { f =>
          f.foreach { nf =>
            val normalized = FeatureNameNormalization.normalize(nf.name)
            builder.putFeature(normalized, nf.f)
          }
        }
        Example
          .newBuilder()
          .setFeatures(builder.build())
          .build()
      }
  }
}
