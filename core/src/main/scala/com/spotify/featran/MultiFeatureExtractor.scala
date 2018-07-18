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

import scala.reflect.ClassTag

/**
 * Encapsulate features extracted from a [[MultiFeatureSpec]].
 * Allows separation back into specs by names or vectors.
 * @tparam M input collection type, e.g. `Array`, List
 * @tparam T input record type to extract features from
 */
class MultiFeatureExtractor[M[_]: CollectionType, T] private[featran] (
  private val fs: M[MultiFeatureSet[T]],
  @transient private val input: M[T],
  @transient private val settings: Option[M[String]])
    extends Serializable {
  import CollectionType.ops._

  private[this] val extractor =
    new FeatureExtractor(fs.asInstanceOf[M[FeatureSet[T]]], input, settings)

  /**
   * JSON settings of the [[MultiFeatureSpec]] and aggregated feature summary.
   *
   * This can be used with [[MultiFeatureSpec.extractWithSettings]] to bypass the `reduce` step
   * when extracting new records of the same type.
   */
  @transient lazy val featureSettings: M[String] = extractor.featureSettings

  /**
   * Names of the extracted features, in the same order as values in [[featureValues]].
   */
  @transient lazy val featureNames: M[Seq[Seq[String]]] =
    extractor.aggregate.cross(fs).map(x => x._2.multiFeatureNames(x._1))

  /**
   * Values of the extracted features, in the same order as names in [[featureNames]].
   * @tparam F output data type, e.g. `Array[Float]`, `Array[Double]`, `DenseVector[Float]`,
   *           `DenseVector[Double]`
   */
  def featureValues[F: FeatureBuilder: ClassTag]: M[Seq[F]] =
    featureResults.map(_._1)

  /**
   * Values of the extracted features, in the same order as names in [[featureNames]] with
   * rejections keyed on feature name and the original input record.
   * @tparam F output data type, e.g. `Array[Float]`, `Array[Double]`, `DenseVector[Float]`,
   *           `DenseVector[Double]`
   */
  def featureResults[F: FeatureBuilder: ClassTag]
    : M[(Seq[F], Seq[Map[String, FeatureRejection]], T)] = {
    extractor.as
      .cross(extractor.aggregate)
      .cross(fs)
      .map {
        case (((o, a), c), featureSet) =>
          val fb = featureSet.multiFeatureBuilders
          featureSet.multiFeatureValues(a, c, fb)
          (fb.map(_.result).toSeq, fb.map(_.rejections), o)
      }
  }

}
