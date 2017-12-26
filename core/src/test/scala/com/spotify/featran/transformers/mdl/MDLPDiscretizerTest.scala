package com.spotify.featran.transformers.mdl

import org.scalatest.{FlatSpec, Matchers}

class MDLPDiscretizerTest extends FlatSpec with Matchers {
  import com.spotify.featran.transformers.mdl.TestUtility._

  it should "Run MDLPD on single mpg column in cars data  (maxBins = 10)" in {
    val cars = readCars
    val data = cars.map(v => (v.origin, v.mpg))

    val result = new MDLPDiscretizer(data).discretize(10).sorted

    val expected = List(Double.NegativeInfinity, 16.1, 21.05, 30.95, Double.PositiveInfinity)
    assert(expected.length === result.length)
    expected.zip(result).map{case(e, r) => assert(e === r)}
  }

  it should "Run MDLPD on single mpg column in cars data (maxBins = 2)" in {
    val cars = readCars
    val data = cars.map(v => (v.origin, v.mpg))

    val result = new MDLPDiscretizer(data).discretize(2).sorted
    val expected = List(Double.NegativeInfinity, 21.05, Double.PositiveInfinity)

    assert(expected.length === result.length)
    expected.zip(result).map{case(e, r) => assert(e === r)}
  }
}
