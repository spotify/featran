package com.spotify.featran.java.examples;

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
    JFeatureSpec<Record> fs = JFeatureSpec.<Record>of()
        .required(r -> r.d, MinMaxScaler.apply("min-max", 0.0, 1.0))
        .optional(r -> r.s, OneHotEncoder.apply("one-hot"));

    // Extract features from List<Record>
    JFeatureExtractor<Record> f1 = fs.extract(records);

    System.out.println(f1.featureNames());

    // Get feature values as double[]
    for (double[] f : f1.featureValuesDouble()) {
      System.out.println(Arrays.toString(f));
    }

    // Extract settings as a JSON string
    String settings = f1.featureSettings();
    System.out.println(settings);

    // Extract features from new records ,but reuse previously saved settings
    fs.extractWithSettings(randomRecords(), settings);
  }
}
