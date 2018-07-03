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

import com.spotify.featran.{CollectionType, FeatureBuilder, JsonSerializable}
import com.spotify.featran.transformers._
import org.tensorflow.example.{Example, Feature}
import shapeless.datatype.tensorflow.TensorFlowType

import scala.reflect.ClassTag

/**
 * ExampleTransformer takes a TFExample of non-transformed values output by Featran
 * and applies a settings file to transform the Features into the required output object.
 *
 * The main benefit on this approach is that when using this class there does not have to
 * be a JVM dependency on the original scala object or the original spec that were used to
 * create the TFExample and the Settings file.
 *
 *
 */
object ExampleTransformer {
  import TensorFlowType._

  implicit val exampleFlatReader: FlatReader[Example] = new FlatReader[Example]{
    def toFeature(name: String, ex: Example): Option[Feature] = {
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

  def apply[M[_]: CollectionType](settings: M[String]): ExampleTransformer[M] =
    new ExampleTransformer[M](settings)
}

class ExampleTransformer[M[_]: CollectionType](settings: M[String]) extends Serializable {
  import CollectionType.ops._
  import ExampleTransformer._
  import scala.reflect.runtime.universe

  private def cName[T: ClassTag]: String =
    implicitly[ClassTag[T]].runtimeClass.getCanonicalName

  @transient private val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

  private val converters = settings.map { str =>
    val jsonOpt = JsonSerializable[Seq[Settings]].decode(str)
    assert(jsonOpt.isRight, "Unable to parse the settings files.")
    jsonOpt.right.get.map { setting =>
      val transformer = runtimeMirror
        .reflectModule(runtimeMirror.staticModule(setting.cls))
        .instance
        .asInstanceOf[SettingsBuilder]
        .fromSetting(setting)

      (transformer.flatRead[Example], setting.aggregators, transformer)
    }
  }

  val dimSize: M[Int] = converters.map { items =>
    items.map {
      case (_, aggr, tr) =>
        val ta = aggr.map(tr.decodeAggregator)
        tr.unsafeFeatureDimension(ta)
    }.sum
  }

  def transform[F: FeatureBuilder: ClassTag](records: M[Example]): M[F] = {
    val fb = FeatureBuilder[F].newBuilder
    records.cross(converters).cross(dimSize).map {
      case ((record, convs), size) =>
        fb.init(size)
        convs.foreach {
          case (fn, aggr, conv) =>
            val a = aggr.map(conv.decodeAggregator)
            conv.unsafeBuildFeatures(fn(record), a, fb)
        }
        fb.result
    }
  }
}
