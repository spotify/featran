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

import com.spotify.featran.transformers.{Identity, Transformer}

package object converters {
  implicit class RichBoolean(val self: Boolean) extends AnyVal {
    def asDouble: Double = if (self) 1.0 else 0.0
  }

  implicit val identityDefault: DefaultTransform[Double] = new DefaultTransform[Double] {
    def apply(featureName: String): Transformer[Double, _, _] = Identity(featureName)
  }
}
