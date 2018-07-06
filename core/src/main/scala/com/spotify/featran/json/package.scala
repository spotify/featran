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

import com.spotify.featran.transformers.{MDLRecord, WeightedLabel}

/**
 * Package for an intermediate format for feature storage using JSON
 */
package object json {

  implicit val jsonFlatReader: FlatReader[String] = new FlatReader[String] {
    import io.circe._
    import cats.syntax.either._
    import CirceImplicits._

    private def toFeature[T: Decoder: Encoder](name: String): String => Option[T] =
      json =>
        JsonSerializable[Map[String, Option[String]]]
          .decode(json)
          .toOption
          .flatMap { map =>
            map
              .get(name)
              .flatten
              .flatMap(o => JsonSerializable[T].decode(o).toOption)
        }

    override def readDouble(name: String): String => Option[Double] = toFeature[Double](name)

    override def readMdlRecord(name: String): String => Option[MDLRecord[String]] =
      toFeature[MDLRecord[String]](name)

    override def readWeightedLabel(name: String): String => Option[List[WeightedLabel]] =
      toFeature[List[WeightedLabel]](name)

    override def readDoubles(name: String): String => Option[Seq[Double]] =
      toFeature[Seq[Double]](name)

    override def readDoubleArray(name: String): String => Option[Array[Double]] =
      toFeature[Array[Double]](name)

    override def readString(name: String): String => Option[String] = toFeature[String](name)

    override def readStrings(name: String): String => Option[Seq[String]] =
      toFeature[Seq[String]](name)
  }

  implicit val jsonFlatWriter: FlatWriter[String] = new FlatWriter[String] {
    import io.circe._
    import CirceImplicits._

    override type IF = (String, Option[Json])

    override def writeDouble(name: String): Option[Double] => (String, Option[Json]) =
      (v: Option[Double]) => (name, v.map(JsonSerializable[Double].encode))

    override def writeMdlRecord(name: String): Option[MDLRecord[String]] => (String, Option[Json]) =
      (v: Option[MDLRecord[String]]) => (name, v.map(JsonSerializable[MDLRecord[String]].encode))

    override def writeWeightedLabel(
      name: String): Option[Seq[WeightedLabel]] => (String, Option[Json]) =
      (v: Option[Seq[WeightedLabel]]) => (name, v.map(JsonSerializable[Seq[WeightedLabel]].encode))

    override def writeDoubles(name: String): Option[Seq[Double]] => (String, Option[Json]) =
      (v: Option[Seq[Double]]) => (name, v.map(JsonSerializable[Seq[Double]].encode))

    override def writeDoubleArray(name: String): Option[Array[Double]] => (String, Option[Json]) =
      (v: Option[Array[Double]]) => (name, v.map(JsonSerializable[Array[Double]].encode))

    override def writeString(name: String): Option[String] => (String, Option[Json]) =
      (v: Option[String]) => (name, v.map(JsonSerializable[String].encode))

    override def writeStrings(name: String): Option[Seq[String]] => (String, Option[Json]) =
      (v: Option[Seq[String]]) => (name, v.map(JsonSerializable[Seq[String]].encode))

    override def writer: Seq[(String, Option[Json])] => String =
      (v: Seq[(String, Option[Json])]) =>
        JsonSerializable[Seq[(String, Option[Json])]].encode(v).noSpaces
  }
}
