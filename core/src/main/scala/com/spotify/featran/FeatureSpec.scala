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

import com.spotify.featran.transformers.Transformer

import scala.language.{higherKinds, implicitConversions}

object FeatureSpec {

  type ARRAY = Array[Option[Any]]

  def of[T]: Builder[T] = new Builder[T](Array.empty)

  class Builder[T] private[featran] (private val features: Array[Feature[T, _, _, _]]) {
    def required[A](f: T => A)(t: Transformer[A, _, _]): Builder[T] =
      optional(t => Some(f(t)))(t)

    def optional[A](f: T => Option[A], default: Option[A] = None)
                   (t: Transformer[A, _, _]): Builder[T] =
      new Builder[T](this.features :+ new Feature(f, default, t))

    def extract[M[_]: CollectionType](input: M[T]): FeatureExtractor[M, T] =
      new FeatureExtractor[M, T](new FeatureSpec[T](features), input)
  }

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
  def unsafeBuildFeatures(a: Option[Any], c: Option[Any], fb: FeatureBuilder[_]): Unit = {
    transformer.optBuildFeatures(a.asInstanceOf[Option[A]], c.asInstanceOf[Option[C]], fb)
  }

}

class FeatureSpec[T] private (private val features: Array[Feature[T, _, _, _]])
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

}
