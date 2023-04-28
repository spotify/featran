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

import com.spotify.featran.transformers.{MDLRecord, WeightedLabel}
import org.tensorflow.proto.example.{Example, Features}
import org.tensorflow.proto.{example => tf}

package object tensorflow {

  /** [[FeatureBuilder]] for output as TensorFlow `Example` type. */
  implicit def tensorFlowFeatureBuilder: FeatureBuilder[tf.Example] = TensorFlowFeatureBuilder()

  implicit val exampleFlatReader: FlatReader[tf.Example] = new FlatReader[tf.Example] {
    import TensorFlowType._

    def readDouble(name: String): Example => Option[Double] =
      (ex: Example) => toFeature(name, ex).flatMap(v => toDoubles(v).headOption)

    def readMdlRecord(name: String): Example => Option[MDLRecord[String]] =
      (ex: Example) => {
        for {
          labelFeature <- toFeature(name + "_label", ex)
          label <- toStrings(labelFeature).headOption
          valueFeature <- toFeature(name + "_value", ex)
          value <- toDoubles(valueFeature).headOption
        } yield MDLRecord(label, value)
      }

    def readWeightedLabel(name: String): Example => Option[List[WeightedLabel]] =
      (ex: Example) => {
        val labels = for {
          keyFeature <- toFeature(name + "_key", ex).toList
          key <- toStrings(keyFeature)
          valueFeature <- toFeature(name + "_value", ex).toList
          value <- toDoubles(valueFeature)
        } yield WeightedLabel(key, value)
        if (labels.isEmpty) None else Some(labels)
      }

    def readDoubles(name: String): Example => Option[Seq[Double]] =
      (ex: Example) => toFeature(name, ex).map(v => toDoubles(v))

    def readDoubleArray(name: String): Example => Option[Array[Double]] =
      (ex: Example) => toFeature(name, ex).map(v => toDoubles(v).toArray)

    def readString(name: String): Example => Option[String] =
      (ex: Example) => toFeature(name, ex).flatMap(v => toStrings(v).headOption)

    def readStrings(name: String): Example => Option[Seq[String]] =
      (ex: Example) => toFeature(name, ex).map(v => toStrings(v))
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
        v.toList.flatMap(values => List(NamedTFFeature(name, fromDoubles(values).build())))
      }

    override def writeDoubleArray(name: String): Option[Array[Double]] => List[NamedTFFeature] =
      (v: Option[Array[Double]]) =>
        v.toList.flatMap { values =>
          val f = fromDoubles(values.toSeq).build()
          List(NamedTFFeature(name, f))
        }

    override def writeString(name: String): Option[String] => List[NamedTFFeature] =
      (v: Option[String]) => {
        v.toList.flatMap(values => List(NamedTFFeature(name, fromStrings(Seq(values)).build())))
      }

    override def writeStrings(name: String): Option[Seq[String]] => List[NamedTFFeature] =
      (v: Option[Seq[String]]) => {
        v.toList.flatMap(values => List(NamedTFFeature(name, fromStrings(values).build())))
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
