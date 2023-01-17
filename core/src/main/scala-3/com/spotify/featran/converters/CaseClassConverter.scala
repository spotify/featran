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

  inline def toSpec[A <: Product](using
    m: Mirror.ProductOf[A],
    d: DefaultTransform[Double]
  ): FeatureSpec[A] =
    featureSpec[A, m.MirroredElemTypes, m.MirroredElemLabels](0)(FeatureSpec.of[A], d)

  private inline def featureSpec[A <: Product, Elems <: Tuple, Labels <: Tuple](
    n: Int
  )(s: FeatureSpec[A], d: DefaultTransform[Double]): FeatureSpec[A] =
    inline erasedValue[Elems] match {
      case _: (elem *: elems1) =>
        inline erasedValue[Labels] match {
          case _: (label *: labels1) =>
            val name = constValue[label].toString
            val spec = inline erasedValue[elem] match {
              case _: Boolean =>
                s.required(_.productElement(n).asInstanceOf[Boolean].asDouble)(d(name))
              case _: Int =>
                s.required(_.productElement(n).asInstanceOf[Int].toDouble)(d(name))
              case _: Long =>
                s.required(_.productElement(n).asInstanceOf[Long].toDouble)(d(name))
              case _: Short =>
                s.required(_.productElement(n).asInstanceOf[Short].toDouble)(d(name))
              case _: Double =>
                s.required(_.productElement(n).asInstanceOf[Double])(d(name))
              case _: Option[Int] =>
                s.optional(_.productElement(n).asInstanceOf[Option[Int]].map(_.toDouble))(d(name))
              case _: Option[Long] =>
                s.optional(_.productElement(n).asInstanceOf[Option[Long]].map(_.toDouble))(d(name))
              case _: Option[Short] =>
                s.optional(_.productElement(n).asInstanceOf[Option[Short]].map(_.toDouble))(d(name))
              case _: Option[Boolean] =>
                s.optional(_.productElement(n).asInstanceOf[Option[Boolean]].map(_.asDouble))(
                  d(name)
                )
              case _: Option[Double] =>
                s.optional(_.productElement(n).asInstanceOf[Option[Double]])(d(name))
              case _: Seq[Int] =>
                s.required(_.productElement(n).asInstanceOf[Seq[Int]].map(_.toDouble))(
                  VectorIdentity(name)
                )
              case _: Seq[Long] =>
                s.required(_.productElement(n).asInstanceOf[Seq[Long]].map(_.toDouble))(
                  VectorIdentity(name)
                )
              case _: Seq[Short] =>
                s.required(_.productElement(n).asInstanceOf[Seq[Short]].map(_.toDouble))(
                  VectorIdentity(name)
                )
              case _: Seq[Boolean] =>
                s.required(_.productElement(n).asInstanceOf[Seq[Boolean]].map(_.asDouble))(
                  VectorIdentity(name)
                )
              case _: Seq[Double] =>
                s.required(_.productElement(n).asInstanceOf[Seq[Double]])(VectorIdentity(name))
              case _: String =>
                s.required(_.productElement(n).asInstanceOf[String])(OneHotEncoder(name))
              case _: Option[String] =>
                s.optional(_.productElement(n).asInstanceOf[Option[String]])(OneHotEncoder(name))
              case _: Seq[String] =>
                s.required(_.productElement(n).asInstanceOf[Seq[String]])(NHotEncoder(name))
              case _: MDLRecord[String] =>
                s.required(_.productElement(n).asInstanceOf[MDLRecord[String]])(MDL(name))
              case _: Seq[WeightedLabel] =>
                s.required(_.productElement(n).asInstanceOf[Seq[WeightedLabel]])(
                  NHotWeightedEncoder(name)
                )
              case _ => s
            }
            featureSpec[A, elems1, labels1](n + 1)(spec, d)
        }
      case _: EmptyTuple => s
    }

}
