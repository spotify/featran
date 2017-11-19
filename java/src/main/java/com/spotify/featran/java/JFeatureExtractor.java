package com.spotify.featran.java;

import com.spotify.featran.FeatureBuilder;
import com.spotify.featran.FeatureExtractor;
import scala.reflect.ClassTag;

import java.util.List;

/**
 * Java wrapper for {@link FeatureExtractor}.
 *
 * Note that {@code float[]} and {@code double[]} are the only supported as output type.
 */
public class JFeatureExtractor<T> {

  private final FeatureExtractor<List, T> self;

  JFeatureExtractor(FeatureExtractor<List, T> self) {
    this.self = self;
  }

  /**
   * Java wrapper for {@link FeatureExtractor#featureSettings()}.
   */
  public String featureSettings() {
    return JavaOps.featureSettings(self);
  }

  /**
   * Java wrapper for {@link FeatureExtractor#featureNames()}.
   */
  public List<String> featureNames() {
    return JavaOps.featureNames(self);
  }

  /**
   * Java wrapper for {@link FeatureExtractor#featureValues(FeatureBuilder, ClassTag)}.
   */
  public List<float[]> featureValuesFloat() {
    return JavaOps.featureValuesFloat(self);
  }

  /**
   * Java wrapper for {@link FeatureExtractor#featureValues(FeatureBuilder, ClassTag)}.
   */
  public List<double[]> featureValuesDouble() {
    return JavaOps.featureValuesDouble(self);
  }

  /**
   * Java wrapper for {@link FeatureExtractor#featureValues(FeatureBuilder, ClassTag)}.
   */
  public List<FloatSparseArray> featureValuesFloatSparse() {
    return JavaOps.featureValuesFloatSparseArray(self);
  }

  /**
   * Java wrapper for {@link FeatureExtractor#featureValues(FeatureBuilder, ClassTag)}.
   */
  public List<DoubleSparseArray> featureValuesDoubleSparse() {
    return JavaOps.featureValuesDoubleSparseArray(self);
  }

}
