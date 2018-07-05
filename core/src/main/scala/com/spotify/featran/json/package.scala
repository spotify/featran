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
import io.circe._
import io.circe.parser._

/**
 * Package for an intermediate format for feature storage using JSON
 */
package object json {
  implicit val jsonFlatReader: FlatReader[String] = new FlatReader[String] {
    private def toJson(str: String): Option[Json] = {
      val parsed = parse(str)
      if (parsed.isLeft) None else Some(parsed.right.get)
    }

    private def toFeature(str: String, name: String): Option[Json] =
      for {
        keyedJson <- toJson(str)
        jsonObj <- keyedJson.asObject
        feature <- jsonObj.apply(name)
      } yield feature

    override def readDouble(name: String): String => Option[Double] =
      (jsonStr: String) =>
        for {
          feature <- toFeature(jsonStr, name)
          number <- feature.asNumber
        } yield number.toDouble

    override def readMdlRecord(name: String): String => Option[MDLRecord[String]] =
      (jsonStr: String) =>
        for {
          feature <- toFeature(jsonStr, name)
          obj <- feature.asObject
          label <- obj.keys.headOption
          valueObj <- obj.values.headOption
          num <- valueObj.asNumber
        } yield MDLRecord(label, num.toDouble)

    override def readWeightedLabel(name: String): String => Option[List[WeightedLabel]] =
      (jsonStr: String) => {
        val weights = for {
          feature <- toFeature(jsonStr, name).toList
          objs <- feature.asArray.toList.flatten
          obj <- objs.asObject.toList
          name <- obj.keys
          values <- obj.values
          num <- values.asNumber
        } yield WeightedLabel(name, num.toDouble)
        if (weights.isEmpty) None else Some(weights)
      }

    override def readDoubles(name: String): String => Option[Seq[Double]] =
      (jsonStr: String) => {
        val items = for {
          feature <- toFeature(jsonStr, name).toList
          objOpt <- feature.asArray.toList
          obj <- objOpt
          number <- obj.asNumber
        } yield number.toDouble
        if (items.isEmpty) None else Some(items)
      }

    override def readDoubleArray(name: String): String => Option[Array[Double]] = {
      val doubles = readDoubles(name)
      (jsonStr: String) =>
        doubles(jsonStr).map(_.toArray)
    }

    override def readString(name: String): String => Option[String] =
      (jsonStr: String) =>
        for {
          feature <- toFeature(jsonStr, name)
          str <- feature.asString
        } yield str

    override def readStrings(name: String): String => Option[Seq[String]] =
      (jsonStr: String) => {
        val items = for {
          feature <- toFeature(jsonStr, name).toList
          objOpt <- feature.asArray.toList
          obj <- objOpt
          str <- obj.asString
        } yield str
        if (items.isEmpty) None else Some(items)
      }
  }

  implicit val jsonFlatWriter: FlatWriter[String] = new FlatWriter[String] {
    override type IF = (String, Option[Json])

    override def writeDouble(name: String): Option[Double] => (String, Option[Json]) =
      (v: Option[Double]) => (name, v.flatMap(Json.fromDouble))

    override def writeMdlRecord(name: String): Option[MDLRecord[String]] => (String, Option[Json]) =
      (v: Option[MDLRecord[String]]) => {
        val json = for {
          record <- v
          jsonDouble <- Json.fromDouble(record.value)
        } yield Json.obj((record.label, jsonDouble))
        (name, json)
      }

    override def writeWeightedLabel(
      name: String): Option[Seq[WeightedLabel]] => (String, Option[Json]) =
      (v: Option[Seq[WeightedLabel]]) => {
        val json = v.map { weights =>
          val keyValues = weights.flatMap { weight =>
            Json.fromDouble(weight.value).map { w =>
              (weight.name, w)
            }
          }
          Json.fromValues(keyValues.map(t => Json.obj(t)))
        }
        (name, json)
      }

    override def writeDoubles(name: String): Option[Seq[Double]] => (String, Option[Json]) =
      (v: Option[Seq[Double]]) => (name, v.map(r => Json.fromValues(r.flatMap(Json.fromDouble))))

    override def writeDoubleArray(name: String): Option[Array[Double]] => (String, Option[Json]) =
      (v: Option[Array[Double]]) => (name, v.map(r => Json.fromValues(r.flatMap(Json.fromDouble))))

    override def writeString(name: String): Option[String] => (String, Option[Json]) =
      (v: Option[String]) => (name, v.map(Json.fromString))

    override def writeStrings(name: String): Option[Seq[String]] => (String, Option[Json]) =
      (v: Option[Seq[String]]) => (name, v.map(r => Json.fromValues(r.map(Json.fromString))))

    override def writer: Seq[(String, Option[Json])] => String =
      (v: Seq[(String, Option[Json])]) =>
        Json.obj(v.collect { case (n, Some(json)) => (n, json) }: _*).noSpaces
  }
}
