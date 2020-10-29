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

package com.spotify.featran.converters

import com.spotify.featran.FeatureSpec
import com.spotify.featran.transformers._

object CaseClassConverter {
  import scala.compiletime._
  import scala.deriving._

  inline def toSpec[A](using m: Mirror.Of[A], d: DefaultTransform[Double]): FeatureSpec[A] =
    inline m match {
      case p: Mirror.ProductOf[A] => 
        featureSpec[A, p.MirroredElemTypes, p.MirroredElemLabels](0)(FeatureSpec.of[A], d)
    }

  inline def featureSpec[A, Elems <: Tuple, Labels <: Tuple](n: Int)(s: FeatureSpec[A], d: DefaultTransform[Double]): FeatureSpec[A] =
      inline erasedValue[Elems] match {
        case _: (elem *: elems1) =>
          inline erasedValue[Labels] match {
            case _: (label *: labels1) =>
              val name = constValue[label].toString
              val spec = inline erasedValue[elem] match {
                case _: Boolean =>
                  s.required(v => productElement[Boolean](v, n).asDouble)(d(name))
                case _: Int =>
                  s.required(v => productElement[Int](v, n).toDouble)(d(name))
                case _: Long =>
                  s.required(v => productElement[Long](v, n).toDouble)(d(name))
                case _: Short =>
                  s.required(v => productElement[Short](v, n).toDouble)(d(name))
                case _: Double =>
                  s.required(v => productElement[Double](v, n))(d(name))
                case _: Option[Int] =>
                  s.optional(v => productElement[Option[Int]](v, n).map(_.toDouble))(d(name))
                case _: Option[Long] =>
                  s.optional(v => productElement[Option[Long]](v, n).map(_.toDouble))(d(name))
                case _: Option[Short] =>
                  s.optional(v => productElement[Option[Short]](v, n).map(_.toDouble))(d(name))
                case _: Option[Boolean] =>
                  s.optional(v => productElement[Option[Boolean]](v, n).map(_.asDouble))(d(name))
                case _: Option[Double] =>
                  s.optional(v => productElement[Option[Double]](v, n))(d(name))
                case _: Seq[Int] =>
                  s.required(v => productElement[Seq[Int]](v, n).map(_.toDouble))(VectorIdentity(name))
                case _: Seq[Long] =>
                  s.required(v => productElement[Seq[Long]](v, n).map(_.toDouble))(VectorIdentity(name))
                case _: Seq[Short] =>
                  s.required(v => productElement[Seq[Short]](v, n).map(_.toDouble))(VectorIdentity(name))
                case _: Seq[Boolean] =>
                  s.required(v => productElement[Seq[Boolean]](v, n).map(_.asDouble))(VectorIdentity(name))
                case _: Seq[Double] =>
                  s.required(v => productElement[Seq[Double]](v, n))(VectorIdentity(name))
                case _: String =>
                  s.required(v => productElement[String](v, n))(OneHotEncoder(name))
                case _: Option[String] =>
                  s.optional(v => productElement[Option[String]](v, n))(OneHotEncoder(name))
                case _: Seq[String] =>
                  s.required(v => productElement[Seq[String]](v, n))(NHotEncoder(name))
                case _: MDLRecord[String] =>
                  s.required(v => productElement[MDLRecord[String]](v, n))(MDL(name))
                case _: Seq[WeightedLabel] =>
                  s.required(v => productElement[Seq[WeightedLabel]](v, n))(NHotWeightedEncoder(name))
                case _ => s
              }
              featureSpec[A, elems1, labels1](n + 1)(spec, d)
          }
        case _: EmptyTuple => s
    }

}
