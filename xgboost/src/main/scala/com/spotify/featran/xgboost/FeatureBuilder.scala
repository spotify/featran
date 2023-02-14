package com.spotify.featran.xgboost

import com.spotify.featran.{FeatureBuilder, SparseArray}
import ml.dmlc.xgboost4j.LabeledPoint

final private case class LabeledPointFB(
  private val underlying: FeatureBuilder[Array[Float]] = FeatureBuilder[Array[Float]].newBuilder
) extends FeatureBuilder[LabeledPoint] {
  override def init(dimension: Int): Unit =
    underlying.init(dimension)

  override def result: LabeledPoint = {
    val result = underlying.result
    LabeledPoint(0.0f, result.length, null, result)
  }

  override def add(name: String, value: Double): Unit =
    underlying.add(name, value)

  override def skip(): Unit = underlying.skip()

  override def newBuilder: FeatureBuilder[LabeledPoint] = LabeledPointFB()
}

final private case class SparseLabeledPointFB(
  private val underlying: FeatureBuilder[SparseArray[Float]] =
    FeatureBuilder[SparseArray[Float]].newBuilder
) extends FeatureBuilder[SparseLabeledPoint] {
  override def init(dimension: Int): Unit = underlying.init(dimension)

  override def result: SparseLabeledPoint =
    new SparseLabeledPoint(
      0.0f,
      underlying.result.length,
      underlying.result.indices,
      underlying.result.values
    )

  override def add(name: String, value: Double): Unit =
    underlying.add(name, value)

  override def skip(): Unit = underlying.skip()

  override def newBuilder: FeatureBuilder[SparseLabeledPoint] = SparseLabeledPointFB()

}
