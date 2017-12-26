package com.spotify.featran.transformers.mdl

import org.scalatest.{FlatSpec, Matchers}

class ThresholdFinderTest extends FlatSpec with Matchers {
  it should "Run few values finder on nLabels = 3 feature len = 4" in {

    val finder = new ThresholdFinder(
      nLabels = 3,
      stoppingCriterion = 0,
      maxBins = 100,
      minBinWeight = 1)

    val feature: Array[(Float, Array[Long])] = Array(
      (5.0f, Array(1L, 2L, 3L)),
      (4.0f, Array(5L, 4L, 20L)) ,
      (3.5f, Array(3L, 20L, 12L)),
      (3.0f, Array(8L, 18L, 2L))
    )

    val result = finder.findThresholds(feature)
    assertResult("-Infinity, 4.0, Infinity") {
      result.mkString(", ")
    }
  }
}
