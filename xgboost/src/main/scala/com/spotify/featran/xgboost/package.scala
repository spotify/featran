/*
 * Copyright 2018 Spotify AB.
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

import ml.dmlc.xgboost4j.LabeledPoint

package object xgboost {

  /**
   * [[FeatureBuilder]] for output as XGBoost's [[LabeledPoint]] type.
   *
   * NOTE: [[LabeledPoint]] stores values as [[Float]]s, so you might loose precision by moving from
   * [[Double]]s to [[Float]]s.
   */
  implicit def denseXGBoostLabeledPointFeatureBuilder
  : FeatureBuilder[LabeledPoint] = new FeatureBuilder[LabeledPoint] {
    private val denseArrayFB = implicitly[FeatureBuilder[Array[Float]]]
    override def init(dimension: Int): Unit = denseArrayFB.init(dimension)
    override def result: LabeledPoint = LabeledPoint(0.0f, null, denseArrayFB.result)
    override def add(name: String, value: Double): Unit = denseArrayFB.add(name, value)
    override def skip(): Unit = denseArrayFB.skip()
  }

  /**
   * [[FeatureBuilder]] for output as XGBoost's sparse [[LabeledPoint]] type.
   *
   * NOTE: [[LabeledPoint]] stores values as [[Float]]s, so you might loose precision by moving from
   * [[Double]]s to [[Float]]s.
   */
  implicit def sparseXGBoostLabeledPointFeatureBuilder
  : FeatureBuilder[SparseLabeledPoint] = new FeatureBuilder[SparseLabeledPoint] {
    private val sparseArrayFB = implicitly[FeatureBuilder[SparseArray[Float]]]
    override def init(dimension: Int): Unit = sparseArrayFB.init(dimension)
    override def result: SparseLabeledPoint =
      new SparseLabeledPoint(0.0f, sparseArrayFB.result.indices, sparseArrayFB.result.values)
    override def add(name: String, value: Double): Unit = sparseArrayFB.add(name, value)
    override def skip(): Unit = sparseArrayFB.skip()
  }

}
