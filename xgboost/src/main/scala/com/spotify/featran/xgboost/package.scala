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
   * [[FeatureBuilder]] for output as XGBoost's `LabeledPoint` type.
   *
   * NOTE: `LabeledPoint` stores values as `Float`s, so you might loose precision by moving from
   * `Double`s to `Float`s.
   */
  implicit def denseXGBoostLabeledPointFeatureBuilder: FeatureBuilder[LabeledPoint] =
    LabeledPointFB()

  /**
   * [[FeatureBuilder]] for output as XGBoost's sparse `LabeledPoint` type.
   *
   * NOTE: `LabeledPoint` stores values as `Float`s, so you might loose precision by moving from
   * `Double`s to `Float`s.
   */
  implicit def sparseXGBoostLabeledPointFeatureBuilder: FeatureBuilder[SparseLabeledPoint] =
    SparseLabeledPointFB()
}
