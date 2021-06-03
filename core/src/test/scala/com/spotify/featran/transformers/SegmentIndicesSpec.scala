package com.spotify.featran.transformers

import com.spotify.featran.FeatureSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object SegmentIndicesSpec extends AnyFlatSpec with Matchers {

  def main(args: Array[String]): Unit = {
    def randomMonotonicIncreasingArray(): Array[Int] = {
      val emptyArray = Array.fill(10)(0)
      for (index <- 1 until emptyArray.length) {
        if (math.random() > 0.5) {
          emptyArray(index) = emptyArray(index - 1) + 1
        } else
          emptyArray(index) = emptyArray(index - 1)
      }
      emptyArray
    }

    val hundredRandomArrays = (1 to 100).toList.map(_ => randomMonotonicIncreasingArray())

    val segmentedIndices = FeatureSpec
      .of[Array[Int]]
      .required(identity)(SegmentIndices("segmented"))

    val expected = hundredRandomArrays.map { testCase =>
      testCase.groupBy(identity).toSeq.sortBy(_._1).flatMap { case (_, sameNumber) =>
        sameNumber.indices.toList }
    }

    val result = segmentedIndices.extract(hundredRandomArrays).featureValues[Seq[Int]]


    result should equal(expected)
  }
//  property("default") = Prop.forAll(list[Array[Int]].arbitrary) {
//    (xs) => //Need test input to be non-strict monotonic increasing
//
//  }
}
