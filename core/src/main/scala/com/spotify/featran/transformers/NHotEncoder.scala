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

object NHotEncoder {
  // Missing value = [0.0, 0.0, ...]
  def apply(name: String): Transformer[Seq[String], Set[String], Array[String]] =
    new NHotEncoder(name)
}

private class NHotEncoder(name: String)
  extends Transformer[Seq[String], Set[String], Array[String]](name) {
  override val aggregator: Aggregator[Seq[String], Set[String], Array[String]] =
    Aggregators.from[Seq[String]](_.toSet).to(_.toArray.sorted)
  override def featureDimension(c: Array[String]): Int = c.length
  override def featureNames(c: Array[String]): Seq[String] = c.map(name + "_" + _).toSeq
  override def buildFeatures(a: Option[Seq[String]], c: Array[String],
                             fb: FeatureBuilder[_]): Unit = {
    val as = a.toSet.flatten
    c.foreach(s => if (as.contains(s)) fb.add(1.0) else fb.skip())
  }
}
