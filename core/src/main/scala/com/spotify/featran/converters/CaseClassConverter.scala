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

import scala.reflect.runtime.{universe => ru}
import scala.reflect.ClassTag

object CaseClassConverter {
  import ru._
  private def properties[T: TypeTag]: Array[MethodSymbol] = {
    val tpe = ru.typeTag[T].tpe
    tpe.decls.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }.toArray
  }

  // Use index here to work around serialization of the MethodSymbols
  def get[T: TypeTag: ClassTag](a: T, idx: Int): Any = {
    lazy val methods = properties[T]
    val rm = scala.reflect.runtime.currentMirror
    val instanceMirror = rm.reflect(a)
    instanceMirror.reflectMethod(methods(idx)).apply()
  }

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  def toSpec[T <: Product](implicit tt: TypeTag[T],
                           ct: ClassTag[T],
                           d: DefaultTransform[Double]): FeatureSpec[T] =
    properties[T].zipWithIndex
      .foldLeft(FeatureSpec.of[T]) {
        case (s, (m, idx)) =>
          @transient val method = m
          val name = method.name.toString
          method.info.resultType match {
            // Native Types
            case c if c =:= typeOf[Int] =>
              s.required[Double](v => get(v, idx).asInstanceOf[Int].toDouble)(d(name))
            case c if c =:= typeOf[Long] =>
              s.required[Double](v => get(v, idx).asInstanceOf[Long].toDouble)(d(name))
            case c if c =:= typeOf[Short] =>
              s.required[Double](v => get(v, idx).asInstanceOf[Short].toDouble)(d(name))
            case c if c =:= typeOf[Boolean] =>
              s.required[Double](v => get(v, idx).asInstanceOf[Boolean].asDouble)(d(name))
            case c if c =:= typeOf[Double] =>
              s.required[Double](v => get(v, idx).asInstanceOf[Double])(d(name))
            // Optional Native Types
            case c if c =:= typeOf[Option[Int]] =>
              s.optional[Double](v => get(v, idx).asInstanceOf[Option[Int]].map(_.toDouble))(
                d(name))
            case c if c =:= typeOf[Option[Long]] =>
              s.optional[Double](v => get(v, idx).asInstanceOf[Option[Long]].map(_.toDouble))(
                d(name))
            case c if c =:= typeOf[Option[Short]] =>
              s.optional[Double] { v =>
                get(v, idx).asInstanceOf[Option[Short]].map(_.toDouble)
              }(d(name))
            case c if c =:= typeOf[Option[Boolean]] =>
              s.optional[Double] { v =>
                get(v, idx).asInstanceOf[Option[Boolean]].map(_.asDouble)
              }(d(name))
            case c if c =:= typeOf[Option[Double]] =>
              s.optional[Double](v => get(v, idx).asInstanceOf[Option[Double]])(d(name))
            // Seq Native Types
            case c if c <:< typeOf[Seq[Int]] =>
              s.required { v =>
                get(v, idx).asInstanceOf[Seq[Int]].map(_.toDouble)
              }(VectorIdentity(name))
            case c if c <:< typeOf[Seq[Long]] =>
              s.required { v =>
                get(v, idx).asInstanceOf[Seq[Long]].map(_.toDouble)
              }(VectorIdentity(name))
            case c if c <:< typeOf[Seq[Short]] =>
              s.required { v =>
                get(v, idx).asInstanceOf[Seq[Short]].map(_.toDouble)
              }(VectorIdentity(name))
            case c if c <:< typeOf[Seq[Boolean]] =>
              s.required { v =>
                get(v, idx).asInstanceOf[Seq[Boolean]].map(_.asDouble)
              }(VectorIdentity(name))
            case c if c <:< typeOf[Seq[Double]] =>
              s.required(v => get(v, idx).asInstanceOf[Seq[Double]])(VectorIdentity(name))
            // Strings
            case c if c =:= typeOf[String] =>
              s.required[String](v => get(v, idx).asInstanceOf[String])(OneHotEncoder(name))
            case c if c =:= typeOf[Option[String]] =>
              s.optional[String](v => get(v, idx).asInstanceOf[Option[String]])(OneHotEncoder(name))
            case c if c <:< typeOf[Seq[String]] =>
              s.required[Seq[String]](v => get(v, idx).asInstanceOf[Seq[String]])(NHotEncoder(name))
            case c if c <:< typeOf[MDLRecord[String]] =>
              s.required[MDLRecord[String]] { v =>
                get(v, idx).asInstanceOf[MDLRecord[String]]
              }(MDL(name))
            case c if c <:< typeOf[Seq[WeightedLabel]] =>
              s.required[Seq[WeightedLabel]] { v =>
                get(v, idx).asInstanceOf[Seq[WeightedLabel]]
              }(NHotWeightedEncoder(name))
            case c =>
              sys.error("Not matching Conversions for " + m.toString)
          }
      }
  // scalastyle:off cyclomatic.complexity
  // scalastyle:on method.length
}
