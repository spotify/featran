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

import com.spotify.featran.transformers.{ConvertFns, Converter, MDLRecord, WeightedLabel}
import org.tensorflow.example._
import org.tensorflow.{example => tf}

import scala.reflect.runtime.universe._

case class NamedTFFeature(name: String, f: tf.Feature)

package object tensorflow {
  import shapeless.datatype.tensorflow.TensorFlowType._

  case class TensorFlowFeatureBuilder(
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
      val normalized = name.replaceAll("[^A-Za-z0-9_]", "_")
      underlying.putFeature(normalized, feature)
    }
    override def skip(): Unit = Unit
    override def skip(n: Int): Unit = Unit
    override def result: tf.Example =
      tf.Example.newBuilder().setFeatures(underlying).build()

    override def newBuilder: FeatureBuilder[Example] = TensorFlowFeatureBuilder()
  }

  /**
   * [[FeatureBuilder]] for output as TensorFlow `Example` type.
   */
  implicit def tensorFlowFeatureBuilder: FeatureBuilder[tf.Example] = TensorFlowFeatureBuilder()

  // scalastyle:off
  implicit val tfConverter: Converter[tf.Example] = new Converter[tf.Example] {
    type RT = List[NamedTFFeature]
    def apply[T, A](name: String, typ: Type, fn: T => Option[A]): T => List[NamedTFFeature] = {
      typ match {
        case t if t =:= typeOf[Double] =>
          t: T =>
            {
              fn(t)
                .map { v =>
                  List(NamedTFFeature(name, fromDoubles(Seq(v.asInstanceOf[Double])).build()))
                }
                .getOrElse(Nil)
            }

        case t if t =:= typeOf[String] =>
          t: T =>
            {
              fn(t)
                .map { v =>
                  List(NamedTFFeature(name, fromStrings(Seq(v.asInstanceOf[String])).build()))
                }
                .getOrElse(Nil)
            }

        case t if t <:< typeOf[Seq[String]] =>
          t: T =>
            {
              fn(t)
                .map { v =>
                  List(NamedTFFeature(name, fromStrings(v.asInstanceOf[Seq[String]]).build()))
                }
                .getOrElse(Nil)
            }

        case t if t <:< typeOf[Seq[Double]] =>
          t: T =>
            {
              fn(t)
                .map { v =>
                  List(NamedTFFeature(name, fromDoubles(v.asInstanceOf[Seq[Double]]).build()))
                }
                .getOrElse(Nil)
            }

        case t if t <:< typeOf[Seq[WeightedLabel]] =>
          t: T =>
            {
              fn(t)
                .map { v =>
                  val values = v.asInstanceOf[Seq[WeightedLabel]]
                  List(
                    NamedTFFeature(name + "_key", fromStrings(values.map(_.name)).build()),
                    NamedTFFeature(name + "_value", fromDoubles(values.map(_.value)).build())
                  )
                }
                .getOrElse(Nil)
            }

        case t if t weak_<:< typeOf[MDLRecord[String]] =>
          t: T =>
            {
              fn(t)
                .map { v =>
                  val values = v.asInstanceOf[MDLRecord[Any]]
                  List(
                    NamedTFFeature(name + "_label",
                                   fromStrings(Seq(values.label.toString)).build()),
                    NamedTFFeature(name + "_value", fromDoubles(Seq(values.value)).build())
                  )
                }
                .getOrElse(Nil)
            }

        case _ =>
          v: T =>
            Nil
      }
    }

    def convert[T](row: T, fns: ConvertFns[T, List[NamedTFFeature]]): tf.Example = {
      val builder = Features.newBuilder()
      fns.fns.foreach { f =>
        f(row).foreach { nf =>
          builder.putFeature(nf.name, nf.f)
        }
      }
      Example
        .newBuilder()
        .setFeatures(builder.build())
        .build()
    }
  }
}
