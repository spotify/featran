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

package com.spotify.featran.transformers

import java.net.{URLDecoder, URLEncoder}

import com.spotify.featran.{FeatureBuilder, FeatureRejection, FlatReader, FlatWriter}
import com.twitter.algebird.Aggregator

import scala.collection.SortedMap

/**
 * Transform a collection of categorical features to binary columns, with at most a single
 * one-value.
 *
 * Missing values are either transformed to zero vectors or encoded as a missing value.
 *
 * When using aggregated feature summary from a previous session, unseen labels are either
 * transformed to zero vectors or encoded as `__unknown__` (if `encodeMissingValue` is true) and
 * [FeatureRejection.Unseen]] rejections are reported.
 */
object OneHotEncoder extends SettingsBuilder {

  /**
   * Create a new [[OneHotEncoder]] instance.
   */
  def apply(
    name: String,
    encodeMissingValue: Boolean = false): Transformer[String, Set[String], SortedMap[String, Int]] =
    new OneHotEncoder(name, encodeMissingValue)

  /**
   * Create a new [[OneHotEncoder]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[String, Set[String], SortedMap[String, Int]] = {
    val encodeMissingValue = setting.params("encodeMissingValue").toBoolean
    OneHotEncoder(setting.name, encodeMissingValue)
  }
}

private[featran] class OneHotEncoder(name: String, encodeMissingValue: Boolean)
    extends BaseHotEncoder[String](name, encodeMissingValue) {
  override def prepare(a: String): Set[String] = Set(a)

  override def buildFeatures(a: Option[String],
                             c: SortedMap[String, Int],
                             fb: FeatureBuilder[_]): Unit = {
    a match {
      case Some(k) =>
        c.get(k) match {
          case Some(v) =>
            fb.skip(v)
            fb.add(name + '_' + k, 1.0)
            fb.skip(math.max(0, c.size - v - 1))
            if (encodeMissingValue) fb.skip()
          case None =>
            addMissingItem(c, fb)
            fb.reject(this, FeatureRejection.Unseen(Set(k)))
        }
      case None => addMissingItem(c, fb)
    }
  }

  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readString(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[String] => fw.IF =
    fw.writeString(name)
}

private[featran] object MissingValue {
  val MissingValueToken = "__missing__"
}

private[featran] abstract class BaseHotEncoder[A](name: String, encodeMissingValue: Boolean)
    extends Transformer[A, Set[String], SortedMap[String, Int]](name) {

  import MissingValue.MissingValueToken

  def prepare(a: A): Set[String]

  def addMissingItem(c: SortedMap[String, Int], fb: FeatureBuilder[_]): Unit = {
    fb.skip(c.size)
    if (encodeMissingValue) {
      fb.add(name + '_' + MissingValueToken, 1.0)
    }
  }

  private def present(reduction: Set[String]): SortedMap[String, Int] = {
    val b = SortedMap.newBuilder[String, Int]
    var i = 0
    val array = reduction.toArray
    java.util.Arrays.sort(array, Ordering[String])
    while (i < array.length) {
      b += array(i) -> i
      i += 1
    }
    b.result()
  }

  override val aggregator: Aggregator[A, Set[String], SortedMap[String, Int]] =
    Aggregators.from[A](prepare).to(present)
  override def featureDimension(c: SortedMap[String, Int]): Int =
    if (encodeMissingValue) c.size + 1 else c.size
  override def featureNames(c: SortedMap[String, Int]): Seq[String] = {
    val names = c.map(name + '_' + _._1)(scala.collection.breakOut)
    if (encodeMissingValue) names :+ (name + '_' + MissingValueToken) else names
  }

  override def encodeAggregator(c: SortedMap[String, Int]): String =
    c.map(e => "label:" + URLEncoder.encode(e._1, "UTF-8")).mkString(",")
  override def decodeAggregator(s: String): SortedMap[String, Int] = {
    val a = s.split(",").filter(_.nonEmpty)
    var i = 0
    val b = SortedMap.newBuilder[String, Int]
    while (i < a.length) {
      b += URLDecoder.decode(a(i).replaceAll("^label:", ""), "UTF-8") -> i
      i += 1
    }
    b.result()
  }

  override def params: Map[String, String] =
    Map("encodeMissingValue" -> encodeMissingValue.toString)
}
