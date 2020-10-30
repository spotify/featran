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

import com.spotify.featran.transformers.Transformer
import simulacrum.typeclass
import scala.annotation.implicitNotFound

/** Default Type Class used by the from generator for Case Class Conversions */
@implicitNotFound("Could not find an instance of DefaultTransform for ${T}")
@typeclass trait DefaultTransform[T] extends Serializable {
  def apply(featureName: String): Transformer[T, _, _]
}

object DefaultTransform {
  /* ======================================================================== */
  /* THE FOLLOWING CODE IS MANAGED BY SIMULACRUM; PLEASE DO NOT EDIT!!!!      */
  /* ======================================================================== */

  /** Summon an instance of [[DefaultTransform]] for `T`. */
  @inline def apply[T](implicit instance: DefaultTransform[T]): DefaultTransform[T] = instance

  object ops {
    implicit def toAllDefaultTransformOps[T](
      target: T
    )(implicit tc: DefaultTransform[T]): AllOps[T] {
      type TypeClassType = DefaultTransform[T]
    } = new AllOps[T] {
      type TypeClassType = DefaultTransform[T]
      val self: T = target
      val typeClassInstance: TypeClassType = tc
    }
  }
  trait Ops[T] extends Serializable {
    type TypeClassType <: DefaultTransform[T]
    def self: T
    val typeClassInstance: TypeClassType
  }
  trait AllOps[T] extends Ops[T]
  trait ToDefaultTransformOps extends Serializable {
    implicit def toDefaultTransformOps[T](target: T)(implicit tc: DefaultTransform[T]): Ops[T] {
      type TypeClassType = DefaultTransform[T]
    } = new Ops[T] {
      type TypeClassType = DefaultTransform[T]
      val self: T = target
      val typeClassInstance: TypeClassType = tc
    }
  }
  object nonInheritedOps extends ToDefaultTransformOps

  /* ======================================================================== */
  /* END OF SIMULACRUM-MANAGED CODE                                           */
  /* ======================================================================== */

}
