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

import org.tensorflow.example.{Example, Feature, Features, FloatList}

package object tensorflow {
  implicit val tfExampleFB: FeatureBuilder[Example] = new FeatureBuilder[Example] {
    @transient private lazy val fb = Features.newBuilder()
    override def init(dimension: Int): Unit = fb.clear()
    override def add(name: String, value: Double): Unit =
      fb.putFeature(
        name,
        Feature.newBuilder()
          .setFloatList(FloatList.newBuilder().addValue(value.toFloat))
          .build())
    override def skip(): Unit = Unit
    override def result: Example = Example.newBuilder().setFeatures(fb).build()
  }
}
