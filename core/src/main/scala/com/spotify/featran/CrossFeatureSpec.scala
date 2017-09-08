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
import scala.language.{higherKinds, implicitConversions}

case class CrossFeatureSpec[T](spec: FeatureSpec[T], cross: List[Cross]) {
  def extract[M[_]: CollectionType](input: M[T]): CrossedFeatureExtractor[M, T] =
    new CrossedFeatureExtractor[M, T](new FeatureSet[T](spec.features), input, None, cross)

  def extractWithSettings[M[_]: CollectionType](input: M[T], settings: M[String])
  : CrossedFeatureExtractor[M, T] = {
    val set = new FeatureSet(spec.features)
    new CrossedFeatureExtractor[M, T](set, input, Some(settings), cross)
  }
}

class CrossedFeatureExtractor[M[_]: CollectionType, T] private[featran]
(private val fs: FeatureSet[T],
 @transient private val input: M[T],
 @transient private val settings: Option[M[String]],
 private val cross: List[Cross]
) extends Serializable {

  private val extractor = new FeatureExtractor(fs, input, settings)

  @transient private val dt: CollectionType[M] = implicitly[CollectionType[M]]
  import dt.Ops._

  def featureValues[F: FeatureBuilder : ClassTag : FeatureGetter]: M[F] =
    featureResults.map(_.value)

  def featureResults[F: FeatureBuilder : ClassTag : FeatureGetter]: M[FeatureResult[F, T]] = {
    val fb = implicitly[FeatureBuilder[F]]
    val cls = fs
    val localCross = cross.toArray
    val crosser = new FeatureCross(cls.features)
    extractor
      .featureResults
      .cross(extractor.aggregate)
      .cross(featureNames)
      .map{case((FeatureResult(value, rej, orig), c), names) =>
        val index = cls.featureDimensionIndex(c).toMap
        val values = crosser.crossValues(localCross, names.toArray, value, index)
        FeatureResult(values, rej, orig)
      }
  }

  @transient lazy val featureNames: M[Seq[String]] = {
    val cls = fs
    val localCross = cross.toArray
    val crosser = new FeatureCross(cls.features)
    extractor.aggregate.map { a =>
      val names = cls.featureNames(a)
      val index = cls.featureDimensionIndex(a).toMap
      crosser.crossNames(localCross, names.toArray, index)
    }
  }

  @transient lazy val featureSettings: M[String] = extractor.featureSettings
}
