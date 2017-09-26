package com.spotify.featran.java;

import com.spotify.featran.transformers.MinMaxScaler;
import com.spotify.featran.transformers.OneHotEncoder;
import scala.Tuple2;

import java.util.Optional;

public class JavaTestUtil {

  private JavaTestUtil() {}

  public static JFeatureSpec<Tuple2<String, Integer>> spec() {
    return JFeatureSpec.<Tuple2<String, Integer>>create()
        .required(t -> t._1, OneHotEncoder.apply("one_hot"))
        .required(t -> t._2.doubleValue(), MinMaxScaler.apply("min_max", 0.0, 1.0));
  }

  public static JFeatureSpec<String> optionalSpec() {
    return JFeatureSpec.<String>create()
        .optional(Optional::ofNullable, OneHotEncoder.apply("one_hot"));
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
