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

package com.spotify.featran.xgboost

import ml.dmlc.xgboost4j.LabeledPoint

/**
 * Class to distinguish sparse `LabeledPoint` from its dense type.
 *
 * See `LabeledPoint` doc for field doc.
 */
final class SparseLabeledPoint private[xgboost] (label: Float,
                                                 indices: Array[Int],
                                                 values: Array[Float],
                                                 weight: Float = 1f,
                                                 group: Int = -1,
                                                 baseMargin: Float = Float.NaN)
    extends Serializable {
  require(indices != null, "Indices can't be null")
  val labeledPoint =
    LabeledPoint(label, indices, values, weight, group, baseMargin)
}
