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

import scala.language.{higherKinds, implicitConversions}

/**
 * Companion object for [[MultiFeatureSpec]].
 */
object MultiFeatureSpec {
  def specs[T](specs: FeatureSpec[T]*): MultiFeatureSpec[T] = {
    val nameToSpec = specs.zipWithIndex.flatMap{case(spec, index) =>
      spec.features.map(f => (f.transformer.name, index))
    }.toMap

    new MultiFeatureSpec(nameToSpec, FeatureSpec.combine(specs:_*).features)
  }
}

/**
 * Wrapper for FeatureSpec that allows for combination and seperation of different specs
 */
class MultiFeatureSpec[T](
  private[featran] val mapping: Map[String, Int],
  private[featran] val multiFeatures: Array[Feature[T, _, _, _]])
  extends FeatureSpec[T](multiFeatures) {

  def multiExtract[M[_]: CollectionType](input: M[T]): MultiFeatureExtractor[M, T] =
    new MultiFeatureExtractor[M, T](new FeatureSet[T](features), input, None, mapping)

  def multiExtractWithSettings[M[_]: CollectionType](input: M[T], settings: M[String])
  : MultiFeatureExtractor[M, T] =
    new MultiFeatureExtractor[M, T](new FeatureSet[T](features), input, Some(settings), mapping)
}
