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

package com.spotify.featran.java;

import com.spotify.featran.transformers.MinMaxScaler;
import com.spotify.featran.transformers.OneHotEncoder;
import scala.Tuple2;

import java.util.Optional;

public class JavaTestUtil {

  private JavaTestUtil() {}

  public static JFeatureSpec<Tuple2<String, Integer>> spec() {
    return JFeatureSpec.<Tuple2<String, Integer>>create()
        .required(t -> t._1, OneHotEncoder.apply("one_hot", false))
        .required(t -> t._2.doubleValue(), MinMaxScaler.apply("min_max", 0.0, 1.0));
  }

  public static JFeatureSpec<String> optionalSpec() {
    return JFeatureSpec.<String>create()
        .optional(Optional::ofNullable, OneHotEncoder.apply("one_hot", false));
  }

  public static JFeatureSpec<Tuple2<String, String>> crossSpec() {
    return JFeatureSpec.<Tuple2<String, String>>create()
        .required(t -> t._1, OneHotEncoder.apply("one_hot_a", false))
        .required(t -> t._2, OneHotEncoder.apply("one_hot_b", false))
        .cross("one_hot_a", "one_hot_b", (a, b) -> a * b);
  }

  public static int[] getIndicies(FloatSparseArray a) {
    return a.indices();
  }

  public static int[] getIndicies(DoubleSparseArray a) {
    return a.indices();
  }

  public static float[] getValues(FloatSparseArray a) {
    return a.values();
  }

  public static double[] getValues(DoubleSparseArray a) {
    return a.values();
  }

  public static float[] getDense(FloatSparseArray a) {
    return a.toDense();
  }

  public static double[] getDense(DoubleSparseArray a) {
    return a.toDense();
  }

}
