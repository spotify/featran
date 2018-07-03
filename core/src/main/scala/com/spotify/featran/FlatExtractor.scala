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

package com.spotify.featran

import com.spotify.featran.transformers.{MDLRecord, Settings, SettingsBuilder, WeightedLabel}
import simulacrum.typeclass

import scala.reflect.ClassTag

/**
 * TypeClass that is used to read data from flat files.  The requirement is that each
 * feature comes from the same type and can be looked up by name.
 * @tparam T The intermediate storage format for each feature.
 */
@typeclass trait FlatReader[T] extends Serializable {
  def readDouble(name: String): T => Option[Double]

  def readMdlRecord(name: String): T => Option[MDLRecord[String]]

  def readWeightedLabel(name: String): T => Option[List[WeightedLabel]]

  def readDoubles(name: String): T => Option[Seq[Double]]

  def readDoubleArray(name: String): T => Option[Array[Double]]

  def readString(name: String): T => Option[String]

  def readStrings(name: String): T => Option[Seq[String]]
}

/**
 * Sometimes it is useful to store the features in an intermediate state in normally
 * a flat version like Examples or maybe JSON.  This makes it easier to interface with
 * other systems.
 *
 * This function allows the reading of data from these flat versions by name with a given
 * settings file to extract the final output.
 */
object FlatExtractor {
  def apply[M[_]: CollectionType, T: ClassTag: FlatReader](setCol: M[String]): FlatExtractor[M, T] =
    new FlatExtractor[M, T](setCol)
}

private[featran] class FlatExtractor[M[_]: CollectionType, T: ClassTag: FlatReader](settings: M[String])
    extends Serializable {

  import CollectionType.ops._
  import scala.reflect.runtime.universe

  @transient private val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

  private val converters = settings.map { str =>
    val jsonOpt = JsonSerializable[Seq[Settings]].decode(str)
    assert(jsonOpt.isRight, "Unable to parse the settings files.")
    jsonOpt.right.get.map { setting =>
      val transformer = runtimeMirror
        .reflectModule(runtimeMirror.staticModule(setting.cls))
        .instance
        .asInstanceOf[SettingsBuilder]
        .fromSettings(setting)

      (transformer.flatRead[T], setting.aggregators, transformer)
    }
  }

  private val dimSize: M[Int] = converters.map { items =>
    items.map {
      case (_, aggr, tr) =>
        val ta = aggr.map(tr.decodeAggregator)
        tr.unsafeFeatureDimension(ta)
    }.sum
  }

  def extract[F: FeatureBuilder: ClassTag](records: M[T]): M[F] = {
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
