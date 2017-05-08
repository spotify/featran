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

package com.spotify.featran.transformers

import com.spotify.featran.FeatureBuilder
import com.twitter.algebird.Aggregator

abstract class Transformer[A, B, C](val name: String) extends Serializable {

  val aggregator: Aggregator[A, B, C]

  // number of generated features
  def featureDimension(c: C): Int

  // names of the generated features
  def featureNames(c: C): Seq[String]

  // build features
  def buildFeatures(a: Option[A], c: C, fb: FeatureBuilder[_]): Unit

  //================================================================================
  // Special cases when value is missing in all rows
  //================================================================================

  def optFeatureDimension(c: Option[C]): Int = c match {
    case Some(x) => featureDimension(x)
    case None => 1
  }

  def optFeatureNames(c: Option[C]): Seq[String] = c match {
    case Some(x) => featureNames(x)
    case None => Seq(name)
  }

  def optBuildFeatures(a: Option[A], c: Option[C], fb: FeatureBuilder[_]): Unit = c match {
    case Some(x) => buildFeatures(a, x, fb)
    case None => fb.skip()
  }

}
