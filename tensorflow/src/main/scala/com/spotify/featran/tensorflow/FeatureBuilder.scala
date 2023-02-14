package com.spotify.featran.tensorflow

import com.spotify.featran.FeatureBuilder
import org.tensorflow.proto.{example => tf}

import scala.annotation.nowarn

final case class TensorFlowFeatureBuilder(
  @transient private var underlying: tf.Features.Builder = tf.Features.newBuilder()
) extends FeatureBuilder[tf.Example] {
  override def init(dimension: Int): Unit = {
    if (underlying == null) {
      underlying = tf.Features.newBuilder()
    }
    underlying.clear(): @nowarn
  }

  override def add(name: String, value: Double): Unit = {
    val feature = tf.Feature
      .newBuilder()
      .setFloatList(tf.FloatList.newBuilder().addValue(value.toFloat))
      .build()
    val normalized = FeatureNameNormalization.normalize(name)
    underlying.putFeature(normalized, feature): @nowarn
  }

  override def skip(): Unit = ()

  override def skip(n: Int): Unit = ()

  override def result: tf.Example =
    tf.Example.newBuilder().setFeatures(underlying).build()

  override def newBuilder: FeatureBuilder[tf.Example] = TensorFlowFeatureBuilder()
}
