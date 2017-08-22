package com.spotify.featran.java;

import com.spotify.featran.FeatureExtractor;

import java.util.List;

public class JFeatureExtractor<T> {

  private final FeatureExtractor<List, T> self;

  JFeatureExtractor(FeatureExtractor<List, T> self) {
    this.self = self;
  }

  public String featureSettings() {
    return JavaOps.featureSettings(self);
  }

  public List<String> featureNames() {
    return JavaOps.featureNames(self);
  }

  public List<float[]> featureValuesFloat() {
    return JavaOps.featureValuesFloat(self);
  }

  public List<double[]> featureValuesDouble() {
    return JavaOps.featureValuesDouble(self);
  }

}
