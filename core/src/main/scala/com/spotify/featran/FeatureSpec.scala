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

import com.spotify.featran.transformers.{Settings, Transformer}

import scala.language.{higherKinds, implicitConversions}

/**
 * Companion object for [[FeatureSpec]].
 */
object FeatureSpec {

  private[featran] type ARRAY = Array[Option[Any]]

  /**
   * Create a new [[FeatureSpec]] for input record type `T`.
   * @tparam T input record type to extract features from
   */
  def of[T]: FeatureSpec[T] = new FeatureSpec[T](Array.empty)

}

/**
 * Encapsulate specification for feature extraction and transformation.
 * @tparam T input record type to extract features from
 */
class FeatureSpec[T] private[featran] (private val features: Array[Feature[T, _, _, _]]) {

  /**
   * Add a required field specification.
   * @param f function to extract feature `A` from record `T`
   * @param t [[com.spotify.featran.transformers.Transformer Transformer]] for extracted feature `A`
   * @tparam A extracted feature type
   */
  def required[A](f: T => A)(t: Transformer[A, _, _]): FeatureSpec[T] =
    optional(t => Some(f(t)))(t)

  /**
   * Add an optional field specification.
   * @param f function to extract feature `Option[A]` from record `T`
   * @param default default for missing values
   * @param t [[com.spotify.featran.transformers.Transformer Transformer]] for extracted feature `A`
   * @tparam A extracted feature type
   */
  def optional[A](f: T => Option[A], default: Option[A] = None)
                 (t: Transformer[A, _, _]): FeatureSpec[T] =
    new FeatureSpec[T](this.features :+ new Feature(f, default, t))

  /**
   * Extract features from a input collection.
   *
   * This is done in two steps, a `reduce` step over the collection to aggregate feature summary,
   * and a `map` step to transform values using the summary.
   * @param input input collection
   * @tparam M input collection type, e.g. `Array`, `List`
   */
  def extract[M[_]: CollectionType](input: M[T]): FeatureExtractor[M, T] =
    new FeatureExtractor[M, T](new FeatureSet[T](features), input, None)

  /**
   * Extract features from a input collection using settings from a previous session.
   *
   * This bypasses the `reduce` step in [[extract]] and uses feature summary from settings exported
   * in a previous session.
   * @param input input collection
   * @param settings JSON settings from a previous session
   * @tparam M input collection type, e.g. `Array`, `List`
   */
  def extractWithSettings[M[_]: CollectionType](input: M[T], settings: M[String])
  : FeatureExtractor[M, T] =
    new FeatureExtractor[M, T](new FeatureSet[T](features), input, Some(settings))

}

private class Feature[T, A, B, C](val f: T => Option[A],
                                  val default: Option[A],
                                  val transformer: Transformer[A, B, C])
  extends Serializable {

  def get(t: T): Option[A] = f(t).orElse(default)

  // Option[A] => Option[B]
  def unsafePrepare(a: Option[Any]): Option[B] =
    a.asInstanceOf[Option[A]].map(transformer.aggregator.prepare)

  // (Option[B], Option[B]) => Option[B]
  def unsafeSum(x: Option[Any], y: Option[Any]): Option[Any] =
    (x.asInstanceOf[Option[B]], y.asInstanceOf[Option[B]]) match {
      case (Some(a), Some(b)) => Some(transformer.aggregator.semigroup.plus(a, b))
      case (Some(a), None) => Some(a)
      case (None, Some(b)) => Some(b)
      case _ => None
    }

  // Option[B] => Option[C]
  def unsafePresent(b: Option[Any]): Option[C] =
    b.asInstanceOf[Option[B]].map(transformer.aggregator.present)

  // Option[C] => Int
  def unsafeFeatureDimension(c: Option[Any]): Int =
    transformer.optFeatureDimension(c.asInstanceOf[Option[C]])

  // Option[C] => Array[String]
  def unsafeFeatureNames(c: Option[Any]): Seq[String] =
    transformer.optFeatureNames(c.asInstanceOf[Option[C]])

  // (Option[A], Option[C], FeatureBuilder[F])
  def unsafeBuildFeatures(a: Option[Any], c: Option[Any], fb: FeatureBuilder[_]): Unit =
    transformer.optBuildFeatures(a.asInstanceOf[Option[A]], c.asInstanceOf[Option[C]], fb)

  // Option[C]
  def unsafeSettings(c: Option[Any]): Settings = transformer.settings(c.asInstanceOf[Option[C]])

}

private class FeatureSet[T](private val features: Array[Feature[T, _, _, _]])
  extends Serializable {

  {
    val (_, dups) = features
      .foldLeft((Set.empty[String], Set.empty[String])) { case ((u, d), f) =>
        val n = f.transformer.name
        if (u.contains(n)) {
          (u, d + n)
        } else {
          (u + n, d)
        }
      }
    require(dups.isEmpty, "duplicate transformer names: " + dups.mkString(", "))
  }

  import FeatureSpec.ARRAY

  private val n = features.length

  // T => Array[Option[A]]
  def unsafeGet(t: T): ARRAY = features.map(_.get(t))

  // Array[Option[A]] => Array[Option[B]]
  def unsafePrepare(a: ARRAY): ARRAY = {
    require(n == a.length)
    var i = 0
    val r = Array.fill[Option[Any]](n)(null)
    while (i < n) {
      r(i) = features(i).unsafePrepare(a(i))
      i += 1
    }
    r
  }

  // (Array[Option[B]], Array[Option[B]]) => Array[Option[B]]
  def unsafeSum(lhs: ARRAY, rhs: ARRAY): ARRAY = {
    require(n == lhs.length)
    require(n == rhs.length)
    val r = Array.fill[Option[Any]](n)(null)
    var i = 0
    while (i < n) {
      r(i) = features(i).unsafeSum(lhs(i), rhs(i))
      i += 1
    }
    r
  }

  // Array[Option[B]] => Array[Option[C]]
  def unsafePresent(b: ARRAY): ARRAY = {
    require(n == b.length)
    var i = 0
    val r = Array.fill[Option[Any]](n)(null)
    while (i < n) {
      r(i) = features(i).unsafePresent(b(i))
      i += 1
    }
    r
  }

  // Array[Option[C]] => Int
  def featureDimension(c: ARRAY): Int = {
    require(n == c.length)
    var sum = 0
    var i = 0
    while (i < n) {
      sum += features(i).unsafeFeatureDimension(c(i))
      i += 1
    }
    sum
  }

  // Array[Option[C]] => Array[String]
  def featureNames(c: ARRAY): Seq[String] = {
    require(n == c.length)
    val b = Seq.newBuilder[String]
    var i = 0
    while (i < n) {
      features(i).unsafeFeatureNames(c(i)).foreach(b += _)
      i += 1
    }
    b.result()
  }

  // (Array[Option[A]], Array[Option[C]], FeatureBuilder[F])
  def featureValues[F](a: ARRAY, c: ARRAY, fb: FeatureBuilder[F]): Unit = {
    require(n == c.length)
    fb.init(featureDimension(c))
    var i = 0
    while (i < n) {
      features(i).unsafeBuildFeatures(a(i), c(i), fb)
      i += 1
    }
  }

  // Option[C]
  def featureSettings(c: ARRAY): Seq[Settings] = {
    require(n == c.length)
    val b = Seq.newBuilder[Settings]
    var i = 0
    while (i < n) {
      b += features(i).unsafeSettings(c(i))
      i += 1
    }
    b.result()
  }

  def decodeAggregators(s: Seq[Settings]): ARRAY = {
    val r = new Array[Option[Any]](n)
    var i = 0
    val it = s.iterator
    while (i < n) {
      r(i) = features(i).transformer.decodeAggregator(it.next().aggregators)
      i += 1
    }
    r
  }

}
