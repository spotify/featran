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

package com.spotify.featran.transformers

import java.util.{TreeMap => JTreeMap}

import breeze.linalg._
import com.spotify.featran._
import com.twitter.algebird.{QTree, QTreeAggregator, QTreeSemigroup}
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck._

object TransformerSpec extends Properties("transformer") {

  implicit def list[T](implicit arb: Arbitrary[T]): Arbitrary[List[T]] =
    Arbitrary(Gen.listOfN(100, arb.arbitrary))

  // Double.NaN != Double.NaN
  // Also map to float to workaround precision error
  private def safeCompare(xs: List[Seq[Double]], ys: List[Seq[Double]]) = {
    def d2e(x: Double): Either[Int, Float] = if (x.isNaN) Left(0) else Right(x.toFloat)
    xs.map(_.map(d2e)) == ys.map(_.map(d2e))
  }

  private def test[T](t: Transformer[T, _, _],
                      input: List[T],
                      names: Seq[String],
                      expected: List[Seq[Double]],
                      missing: Seq[Double],
                      outOfBoundElems: List[(T, Seq[Double])] = Nil): Prop = {
    val fs = FeatureSpec.of[T].required(identity)(t)

    // all values present
    val f1 = fs.extract(input)
    // add one missing value
    val f2 = FeatureSpec.of[Option[T]].optional(identity)(t).extract(input.map(Some(_)) :+ None)

    // extract with settings from a previous session
    val settings = f1.featureSettings
    // first half of the values
    val f3 = fs.extractWithSettings(input.take(input.size / 2), settings)
    // all values plus optional elements out of bound of the previous session
    val f4 = fs.extractWithSettings(outOfBoundElems.map(_._1), settings)

    Prop.all(
      "f1 names" |: f1.featureNames == Seq(names),
      "f2 names" |: f2.featureNames == Seq(names),
      "f3 names" |: f3.featureNames == Seq(names),
      "f4 names" |: f4.featureNames == Seq(names),
      "f1 values" |: safeCompare(f1.featureValues[Seq[Double]], expected),
      "f2 values" |: safeCompare(f2.featureValues[Seq[Double]], expected :+ missing),
      "f3 values" |: safeCompare(f3.featureValues[Seq[Double]], expected.take(input.size / 2)),
      "f4 values" |: safeCompare(f4.featureValues[Seq[Double]], outOfBoundElems.map(_._2)),
      "f2 settings" |: settings == f2.featureSettings,
      "f3 settings" |: settings == f3.featureSettings,
      "f4 settings" |: settings == f4.featureSettings)
  }

  property("binarizer") = Prop.forAll { xs: List[Double] =>
    val expected = xs.map(x => Seq(if (x > 0.0) 1.0 else 0.0))
    test(Binarizer("id"), xs, Seq("id"), expected, Seq(0.0))
  }

  property("binarizer params") = Prop.forAll { (xs: List[Double], threshold: Double) =>
    val expected = xs.map(x => Seq(if (x > threshold) 1.0 else 0.0))
    test(Binarizer("id", threshold), xs, Seq("id"), expected, Seq(0.0))
  }

  private val splitsGen = Gen.choose(3, 10)
    .flatMap(n => Gen.listOfN(n, Arbitrary.arbitrary[Double]))
  property("bucketizer") = Prop.forAll(list[Double].arbitrary, splitsGen) { (xs, sp) =>
    val splits = sp.toArray.sorted
    val upper = splits.max
    val names = (0 until splits.length - 1).map("bucketizer_" + _)
    val missing = (0 until splits.length - 1).map(_ => 0.0)
    val expected = xs.map { x =>
      val offset = if (x == upper) splits.length - 2 else splits.indexWhere(x < _) - 1
      if (offset >= 0) {
        (0 until splits.length - 1).map(i => if (i == offset) 1.0 else 0.0)
      } else {
        missing
      }
    }
    test(Bucketizer("bucketizer", splits), xs, names, expected, missing)
  }

  property("bucketizer upper") = Prop.forAll(list[Double].arbitrary) { xs =>
    val (lower, upper) = (xs.min, xs.max)
    val mean = {
      val (l, u) = (BigDecimal(lower), BigDecimal(upper)) // in case of overflow
      (l + (u - l) / 2).toDouble
    }
    val splits = Array(lower, mean, upper)
    val names = (0 until splits.length - 1).map("bucketizer_" + _)
    val missing = (0 until splits.length - 1).map(_ => 0.0)
    val expected = xs.map { x =>
      val offset = if (x == upper) splits.length - 2 else splits.indexWhere(x < _) - 1
      if (offset >= 0) {
        (0 until splits.length - 1).map(i => if (i == offset) 1.0 else 0.0)
      } else {
        missing
      }
    }
    test(Bucketizer("bucketizer", splits), xs, names, expected, missing)
  }

  property("identity") = Prop.forAll { xs: List[Double] =>
    test(Identity("id"), xs, Seq("id"), xs.map(Seq(_)), Seq(0.0))
  }

  property("max abs") = Prop.forAll { xs: List[Double] =>
    val max = xs.map(math.abs).max
    val expected = xs.map(x => Seq(x / max))
    val oob = List((max + 1, Seq(1.0)), (-max - 1, Seq(-1.0)))
    test(MaxAbsScaler("max_abs"), xs, Seq("max_abs"), expected, Seq(0.0), oob)
  }

  property("min max") = Prop.forAll { xs: List[Double] =>
    val (min, max) = (xs.min, xs.max)
    val delta = max - min
    val expected = xs.map(x => Seq((x - min) / delta))
    val oob = List((min - 1, Seq(0.0)), (max + 1, Seq(1.0))) // exceed lower and upper bounds
    test(MinMaxScaler("min_max"), xs, Seq("min_max"), expected, Seq(0.0), oob)
  }

  // limit the range of min and max to avoid overflow
  private val minMaxGen = for {
    min <- Gen.choose(-1000.0, 1000.0)
    range <- Gen.choose(1.0, 2000.0)
  } yield (min, min + range)
  property("min max params") = Prop.forAll(
    Gen.listOfN(100, Arbitrary.arbitrary[Double]),
    minMaxGen) { (xs: List[Double], p) =>
    val (minP, maxP) = p
    val (min, max) = (xs.min, xs.max)
    val delta = max - min
    val expected = xs.map(x => Seq((x - min) / delta * (maxP - minP) + minP))
    val oob = List((min - 1, Seq(minP)), (max + 1, Seq(maxP))) // exceed lower and upper bounds
    test(MinMaxScaler("min_max", minP, maxP), xs, Seq("min_max"), expected, Seq(minP), oob)
  }

  private val nHotGen = Gen.listOfN(100, Gen.listOfN(10, Gen.alphaStr))
  property("n hot") = Prop.forAll(nHotGen) { xs =>
    val cats = xs.flatten.distinct.sorted
    val names = cats.map("n_hot_" + _)
    val expected = xs.map(s => cats.map(c => if (s.contains(c)) 1.0 else 0.0))
    val missing = cats.map(_ => 0.0)
    val oob = List((List("s1", "s2"), missing)) // unseen labels
    test(NHotEncoder("n_hot"), xs, names, expected, missing, oob)
  }

  private val normGen = Gen.listOfN(100,
    Gen.listOfN(10, Arbitrary.arbitrary[Double]).map(_.toArray))
  property("normalizer") = Prop.forAll(normGen, Gen.choose(1.0, 3.0)) { (xs, p) =>
    val names = (0 until 10).map("norm_" + _)
    val expected = xs.map { x =>
      val dv = DenseVector(x)
      (dv / norm(dv, p)).data.toSeq
    }
    val missing = (0 until 10).map(_ => 0.0)
    val oob = List((xs.head :+ 1.0, missing)) // vector of different dimension
    test(Normalizer("norm", p), xs, names, expected, missing, oob)
  }

  private val oneHotGen = Gen.listOfN(100, Gen.alphaStr)
  property("one hot") = Prop.forAll(oneHotGen) { xs =>
    val cats = xs.distinct.sorted
    val names = cats.map("one_hot_" + _)
    val expected = xs.map(s => cats.map(c => if (s == c) 1.0 else 0.0))
    val missing = cats.map(_ => 0.0)
    val oob = List(("s1", missing), ("s2", missing)) // unseen labels
    test(OneHotEncoder("one_hot"), xs, names, expected, missing, oob)
  }

  private val polyGen = Gen.choose(2, 4)
    .flatMap(n => Gen.listOfN(100, Gen.listOfN(n, Arbitrary.arbitrary[Double]).map(_.toArray)))
  property("poly") = Prop.forAll(polyGen, Gen.choose(2, 4)) { (xs, degree) =>
    val dim = PolynomialExpansion.expand(xs.head, degree).length
    val names = (0 until dim).map("poly_" + _)
    val expected = xs.map(v => PolynomialExpansion.expand(v, degree).toSeq)
    val missing = (0 until dim).map(_ => 0.0)
    val oob = List((xs.head :+ 1.0, missing)) // vector of different dimension
    test(PolynomialExpansion("poly", degree), xs, names, expected, missing, oob)
  }

  private val quantileGen = Gen.listOfN(100, Gen.posNum[Double])
  property("quantile") = Prop.forAll(quantileGen, Gen.oneOf(2, 4, 5)) { (xs, numBuckets) =>
    // FIXME: make this a black box
    val qt = xs.map(QTree(_)).reduce(new QTreeSemigroup[Double](QTreeAggregator.DefaultK).plus)
    val m = new JTreeMap[Double, Int]()
    val interval = 1.0 / numBuckets
    for (i <- 1 until numBuckets) {
      val (l, u) = qt.quantileBounds(interval * i)
      val k = l + (u - l) / 2
      if (!m.containsKey(k)) {
        m.put(k, i - 1)
      }
    }
    m.put(qt.upperBound, numBuckets - 1)
    val expected = xs.map { x =>
      (0 until numBuckets).map(i => if (i == m.higherEntry(x).getValue) 1.0 else 0.0)
    }
    val names = (0 until numBuckets).map("quantile_" + _)
    val missing = (0 until numBuckets).map(_ => 0.0)
    val oob = List(
      (xs.max + 1, (0 until numBuckets - 1).map(_ => 0.0) :+ 1.0),
      (math.max(xs.min - 1, 0.0), 1.0 +: (0 until numBuckets - 1).map(_ => 0.0))
    )
    test(QuantileDiscretizer("quantile", numBuckets), xs, names, expected, missing, oob)
  }

  def meanAndStddev(xs: List[Double]): (Double, Double) = {
    // breeze.stats.stddev is sample stddev
    val mean = breeze.stats.mean(xs)
    (mean, math.sqrt(xs.map(x => math.pow(x - mean, 2)).sum / xs.size))
  }

  property("standard") = Prop.forAll { xs: List[Double] =>
    val (mean, stddev) = meanAndStddev(xs)
    val expected = xs.map(x => Seq((x - mean) / stddev + mean))
    test(StandardScaler("std"), xs, Seq("std"), expected, Seq(mean))
  }

  property("standard true true") = Prop.forAll { xs: List[Double] =>
    val (mean, stddev) = meanAndStddev(xs)
    val expected = xs.map(x => Seq((x - mean) / stddev))
    test(StandardScaler("std", true, true), xs, Seq("std"), expected, Seq(0.0))
  }

  property("standard true false") = Prop.forAll { xs: List[Double] =>
    val (mean, stddev) = meanAndStddev(xs)
    val expected = xs.map(x => Seq((x - mean) / stddev + mean))
    test(StandardScaler("std", true, false), xs, Seq("std"), expected, Seq(mean))
  }

  property("standard false true") = Prop.forAll { xs: List[Double] =>
    val (mean, _) = meanAndStddev(xs)
    val expected = xs.map(x => Seq(x - mean))
    test(StandardScaler("std", false, true), xs, Seq("std"), expected, Seq(0.0))
  }

  property("standard false false") = Prop.forAll { xs: List[Double] =>
    val (mean, _) = meanAndStddev(xs)
    val expected = xs.map(Seq(_))
    test(StandardScaler("std", false, false), xs, Seq("std"), expected, Seq(mean))
  }

}
