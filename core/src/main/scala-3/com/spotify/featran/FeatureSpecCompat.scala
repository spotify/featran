/*
 * Copyright 2020 Spotify AB.
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

import com.spotify.featran.converters.{CaseClassConverter, DefaultTransform}

import scala.deriving._

trait FeatureSpecCompat {

  /**
   * Generates a new [[FeatureSpec]] for case class of type `T`. This method defaults the
   * transformers based on the types of the fields.
   *
   * The implicit parameter can be used to change the default of the Transformer used for continuous
   * values. When another isn't supplied Identity will be used.
   */
  inline def from[T <: Product](implicit
    m: Mirror.ProductOf[T],
    dt: DefaultTransform[Double]
  ): FeatureSpec[T] =
    CaseClassConverter.toSpec[T]
}
