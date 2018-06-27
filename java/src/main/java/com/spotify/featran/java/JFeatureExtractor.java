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

import com.spotify.featran.FeatureBuilder;
import com.spotify.featran.FeatureExtractor;
import com.spotify.featran.xgboost.SparseLabeledPoint;
import ml.dmlc.xgboost4j.LabeledPoint;
import org.tensorflow.example.Example;
import scala.reflect.ClassTag;

import java.util.List;

/**
 * Java wrapper for {@link FeatureExtractor}.
 *
 * Note that {@code float[]}, {@code double[]}, {@link FloatSparseArray} and
 * {@link DoubleSparseArray} are the only supported as output type.
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

  /**
   * Java wrapper for {@link FeatureExtractor#featureValues(FeatureBuilder, ClassTag)}.
   */
  public List<FloatNamedSparseArray> featureValuesFloatNamedSparse() {
    return JavaOps.featureValuesFloatNamedSparseArray(self);
  }

  /**
   * Java wrapper for {@link FeatureExtractor#featureValues(FeatureBuilder, ClassTag)}.
   */
  public List<DoubleNamedSparseArray> featureValuesDoubleNamedSparse() {
    return JavaOps.featureValuesDoubleNamedSparseArray(self);
  }

  /**
   * Java wrapper for {@link FeatureExtractor#featureValues(FeatureBuilder, ClassTag)}.
   */
  public List<Example> featureValuesExample() {
    return JavaOps.featureValuesExample(self);
  }

  /**
   * Java wrapper for {@link FeatureExtractor#featureValues(FeatureBuilder, ClassTag)}.
   */
  public List<LabeledPoint> featureValuesLabeledPoint() {
    return JavaOps.featureValuesLabeledPoint(self);
  }

  /**
   * Java wrapper for {@link FeatureExtractor#featureValues(FeatureBuilder, ClassTag)}.
   */
  public List<SparseLabeledPoint> featureValuesSparseLabeledPoint() {
    return JavaOps.featureValuesSparseLabeledPoint(self);
  }

}
