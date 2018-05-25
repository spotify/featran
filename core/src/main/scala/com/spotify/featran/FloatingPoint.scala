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

import simulacrum.typeclass

import scala.language.implicitConversions

/**
 * Type class for floating point primitives.
 */
@typeclass trait FloatingPoint[@specialized(Float, Double) T] extends Serializable {
  def fromDouble(x: Double): T
}

object FloatingPoint {
  implicit val floatFP: FloatingPoint[Float] = new FloatingPoint[Float] {
    override def fromDouble(x: Double): Float = x.toFloat
  }
  implicit val doubleFP: FloatingPoint[Double] = new FloatingPoint[Double] {
    override def fromDouble(x: Double): Double = x
  }
}
