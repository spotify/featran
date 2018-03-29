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

package com.spotify.featran.java.examples;

import com.spotify.featran.java.DoubleSparseArray;
import com.spotify.featran.java.JFeatureExtractor;
import com.spotify.featran.java.JFeatureSpec;
import com.spotify.featran.transformers.MinMaxScaler;
import com.spotify.featran.transformers.OneHotEncoder;

import java.util.*;

public class JavaExample {

  private static class Record {
    private final double d;
    private final Optional<String> s;

    Record(double d, Optional<String> s) {
      this.d = d;
      this.s = s;
    }
  }

  private static final Random rand = new Random();

  private static List<Record> randomRecords() {
    List<Record> records = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Optional<String> s = i % 5 == 0 ? Optional.empty() : Optional.of("s" + rand.nextInt(5));
      records.add(new Record(rand.nextDouble(), s));
    }
    return records;
  }

  public static void main(String[] args) {
    // Random input
    List<Record> records = randomRecords();

    // Start building a feature specification
    JFeatureSpec<Record> fs = JFeatureSpec.<Record>create()
        .required(r -> r.d, MinMaxScaler.apply("min-max", 0.0, 1.0))
        .optional(r -> r.s, OneHotEncoder.apply("one-hot", false));

    // Extract features from List<Record>
    JFeatureExtractor<Record> f1 = fs.extract(records);

    System.out.println(f1.featureNames());

    // Get feature values as double[]
    for (double[] f : f1.featureValuesDouble()) {
      System.out.println(Arrays.toString(f));
    }

    // Get feature values as DoubleSparseArray
    for (DoubleSparseArray f : f1.featureValuesDoubleSparse()) {
      String s = String.format(
          "indices: [%s], values: [%s], length: %d",
          Arrays.toString(f.indices()), Arrays.toString(f.values()), f.length());
      System.out.println(s);
    }

    // Extract settings as a JSON string
    String settings = f1.featureSettings();
    System.out.println(settings);

    // Extract features from new records ,but reuse previously saved settings
    fs.extractWithSettings(randomRecords(), settings);
  }
}
