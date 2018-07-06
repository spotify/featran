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

package com.spotify.featran.json

import com.spotify.featran.transformers.{MDLRecord, WeightedLabel}
import io.circe._
import cats.syntax.either._
import com.spotify.featran.JsonSerializable
import io.circe.parser.{decode => circeDecode}

private[featran] object CirceImplicits {

  implicit val decodeMDLRecord: Decoder[MDLRecord[String]] = new Decoder[MDLRecord[String]] {
    override def apply(c: HCursor): Decoder.Result[MDLRecord[String]] =
      c.keys.toList.flatten.headOption match {
        case None => Left(DecodingFailure("", c.history))
        case Some(label) =>
          for {
            num <- c.downField(label).as[Double]
          } yield MDLRecord(label, num)
      }
  }

  implicit val encodeMDLRecord: Encoder[MDLRecord[String]] = new Encoder[MDLRecord[String]] {
    override def apply(mdlRecord: MDLRecord[String]): Json = Json.obj(
      (mdlRecord.label, Json.fromDoubleOrNull(mdlRecord.value))
    )
  }

  implicit val decodeWeightedLabel: Decoder[WeightedLabel] = new Decoder[WeightedLabel] {
    override def apply(c: HCursor): Decoder.Result[WeightedLabel] =
      c.keys.toList.flatten.headOption match {
        case None => Left(DecodingFailure("", c.history))
        case Some(label) =>
          for {
            num <- c.downField(label).as[Double]
          } yield WeightedLabel(label, num)
      }
  }

  implicit val encodeWeightedLabel: Encoder[WeightedLabel] = new Encoder[WeightedLabel] {
    override def apply(weightedLabel: WeightedLabel): Json = Json.obj(
      (weightedLabel.name, Json.fromDoubleOrNull(weightedLabel.value))
    )
  }

  implicit val decodeGenericMap: Decoder[Map[String, Option[String]]] =
    new Decoder[Map[String, Option[String]]] {
      override def apply(c: HCursor): Decoder.Result[Map[String, Option[String]]] =
        c.keys
          .filter(_.nonEmpty)
          .toRight(DecodingFailure("flat decoding", c.history))
          .map { list =>
            list.map { f =>
              (f, c.downField(f).focus.map(_.noSpaces))
            }.toMap
          }
    }

  implicit val encodeGenericSeq: Encoder[Seq[(String, Option[Json])]] =
    new Encoder[Seq[(String, Option[Json])]] {
      override def apply(seq: Seq[(String, Option[Json])]): Json =
        Json.obj(seq.collect { case (n, Some(json)) => (n, json) }: _*)
    }

  implicit def circeJsonSerializable[T: Decoder: Encoder]: JsonSerializable[T] =
    new JsonSerializable[T] {
      override def encode(t: T): Json = Encoder[T].apply(t)

      override def decode(str: String): Either[Error, T] =
        circeDecode[T](str).left.map(a => a)
    }

}
