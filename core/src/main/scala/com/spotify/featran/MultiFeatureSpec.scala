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

import com.spotify.featran.transformers.Settings

/**
 * Companion object for [[MultiFeatureSpec]].
 */
object MultiFeatureSpec {
  def apply[T](specs: FeatureSpec[T]*): MultiFeatureSpec[T] = {
    val nameToSpec: Map[String, Int] = specs.zipWithIndex.flatMap {
      case (spec, index) =>
        spec.features.map(_.transformer.name -> index)
    }(scala.collection.breakOut)

    new MultiFeatureSpec(nameToSpec,
                         specs.map(_.features).reduce(_ ++ _),
                         specs.map(_.crossings).reduce(_ ++ _))
  }
}

/**
 * Wrapper for [[FeatureSpec]] that allows for combination and separation of different specs.
 */
class MultiFeatureSpec[T](private[featran] val mapping: Map[String, Int],
                          private[featran] val features: Array[Feature[T, _, _, _]],
                          private val crossings: Crossings) {

  /**
   * Extract features from a input collection.
   *
   * This is done in two steps, a `reduce` step over the collection to aggregate feature summary,
   * and a `map` step to transform values using the summary.
   *
   * @param input input collection
   * @tparam M input collection type, e.g. `Array`, `List`
   */
  def extract[M[_]: CollectionType](input: M[T]): MultiFeatureExtractor[M, T] = {
    import CollectionType.ops._

    val fs = input.pure(new MultiFeatureSet[T](features, crossings, mapping))
    new MultiFeatureExtractor[M, T](fs, input, None)
  }

  /**
   * Extract features from an input collection based on the provided predicate
   *
   * @param input input collection
   * @param predicate Function determining whether or not to include the feature
   * @tparam M input collection type, e.g. `Array`, `List`
   */
  def extractSubset[M[_]: CollectionType](input: M[T])(
    predicate: Feature[T, _, _, _] => Boolean): MultiFeatureExtractor[M, T] = {
    import CollectionType.ops._

    val filteredFeatures: Map[String, Feature[T, _, _, _]] =
      features.filter(predicate).map(f => f.transformer.name -> f).toMap
    val filteredMapping = mapping.filterKeys(filteredFeatures.contains)
    val fs = input.pure(
      new MultiFeatureSet[T](filteredFeatures.values.toArray, crossings, filteredMapping))

    new MultiFeatureExtractor[M, T](fs, input, None)
  }

  /**
   * Extract features from a input collection using settings from a previous session.
   *
   * This bypasses the `reduce` step in [[extract]] and uses feature summary from settings exported
   * in a previous session.
   * @param input input collection
   * @param settings JSON settings from a previous session
   * @tparam M input collection type, e.g. `Array`, `List`
   */
  def extractWithSettings[M[_]: CollectionType](
    input: M[T],
    settings: M[String]): MultiFeatureExtractor[M, T] = {
    import CollectionType.ops._

    val fs = input.pure(new MultiFeatureSet(features, crossings, mapping))
    new MultiFeatureExtractor[M, T](fs, input, Some(settings))
  }

  /**
   * Extract features from a input collection using partial settings from a previous session.
   *
   * This bypasses the `reduce` step in [[extract]] and uses feature summary from settings exported
   * in a previous session.
   * @param input input collection
   * @param settings JSON settings from a previous session
   * @tparam M input collection type, e.g. `Array`, `List`
   */
  def extractWithPartialSettings[M[_]: CollectionType](
    input: M[T],
    settings: M[String]): MultiFeatureExtractor[M, T] = {
    import CollectionType.ops._

    val featureSet = settings.map { s =>
      import io.circe.generic.auto._
      import io.circe.parser._

      val settingsJson = decode[Seq[Settings]](s).right.get
      val filteredFeatures: Map[String, Feature[T, _, _, _]] = features
        .filter { f =>
          settingsJson.exists(x => x.name == f.transformer.name)
        }
        .map(f => f.transformer.name -> f)
        .toMap

      val filteredMapping = mapping.filterKeys(filteredFeatures.contains)

      new MultiFeatureSet[T](filteredFeatures.values.toArray, crossings, filteredMapping)
    }

    new MultiFeatureExtractor[M, T](featureSet, input, Some(settings))
  }

}
