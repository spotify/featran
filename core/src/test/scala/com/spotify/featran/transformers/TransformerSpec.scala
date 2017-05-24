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

import com.spotify.featran._
import org.scalacheck._
import org.scalacheck.Prop.BooleanOperators

object TransformerSpec extends Properties("transformer") {

  implicit def list[T](implicit arb: Arbitrary[T]): Arbitrary[List[T]] =
    Arbitrary(Gen.listOfN(100, arb.arbitrary))

  // Double.NaN != Double.NaN
  // Also map to float to workaround precision error
  private def safeCompare(xs: List[Array[Double]], ys: List[Seq[Double]]) = {
    def d2e(x: Double): Either[Int, Float] = if (x.isNaN) Left(0) else Right(x.toFloat)
    xs.map(_.toSeq.map(d2e)) == ys.map(_.map(d2e))
  }

  private def test[T](t: Transformer[T, _, _],
                      input: List[T],
                      names: Seq[String],
                      expected: List[Seq[Double]],
                      missing: Seq[Double]): Prop = {
    // all values present
    val f1 = FeatureSpec.of[T].required(identity)(t).extract(input)
    // add one missing value
    val f2 = FeatureSpec.of[Option[T]].optional(identity)(t).extract(input.map(Some(_)) :+ None)

    Prop.all(
      "required names" |: f1.featureNames == Seq(names),
      "optional names" |: f2.featureNames == Seq(names),
      "required values" |: safeCompare(f1.featureValues[Array[Double]], expected),
      "optional values" |: safeCompare(f2.featureValues[Array[Double]], expected :+ missing))
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
    .flatMap(n => Gen.listOfN(n, Arbitrary.arbDouble.arbitrary))
  property("bucketizer") = Prop.forAll(list[Double].arbitrary, splitsGen) { (xs, sp) =>
    val splits = sp.toArray.sorted
    val names = (0 until splits.length - 1).map("bucketizer_" + _)
    val missing = (0 until splits.length - 1).map(_ => 0.0)
    val expected = xs.map { x =>
      val offset = splits.indexWhere(x < _) - 1
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
    test(MaxAbsScaler("max_abs"), xs, Seq("max_abs"), expected, Seq(0.0))
  }

  property("min max") = Prop.forAll { xs: List[Double] =>
    val (min, max) = (xs.min, xs.max)
    val delta = max - min
    val expected = xs.map(x => Seq((x - min) / delta))
    test(MinMaxScaler("min_max"), xs, Seq("min_max"), expected, Seq(0.0))
  }

  property("min max params") = Prop.forAll { (xs: List[Double], x: Double, y: Double) =>
    val (minP, maxP) = if (x == y) {
      (math.min(x / 2, x), math.max(x / 2, x))
    } else {
      (math.min(x, y), math.max(x, y))
    }
    val (min, max) = (xs.min, xs.max)
    val delta = max - min
    val expected = xs.map(x => Seq((x - min) / delta * (maxP - minP) + minP))
    test(MinMaxScaler("min_max", minP, maxP), xs, Seq("min_max"), expected, Seq(minP))
  }

  private val nHotGen = Gen.listOfN(100, Gen.listOfN(10, Gen.alphaStr))
  property("n hot") = Prop.forAll(nHotGen) { xs =>
    val cats = xs.flatten.distinct.sorted
    val names = cats.map("n_hot_" + _)
    val expected = xs.map(s => cats.map(c => if (s.contains(c)) 1.0 else 0.0))
    val missing = cats.map(_ => 0.0)
    test(NHotEncoder("n_hot"), xs, names, expected, missing)
  }

  private val oneHotGen = Gen.listOfN(100, Gen.alphaStr)
  property("one hot") = Prop.forAll(oneHotGen) { xs =>
    val cats = xs.distinct.sorted
    val names = cats.map("one_hot_" + _)
    val expected = xs.map(s => cats.map(c => if (s == c) 1.0 else 0.0))
    val missing = cats.map(_ => 0.0)
    test(OneHotEncoder("one_hot"), xs, names, expected, missing)
  }

  private val polyGen = Gen.choose(2, 4)
    .flatMap(n => Gen.listOfN(100, Gen.listOfN(n, Arbitrary.arbDouble.arbitrary).map(_.toArray)))
  property("poly") = Prop.forAll(polyGen, Gen.choose(2, 4)) { (xs, degree) =>
    val dim = PolynomialExpansion.expand(xs.head, degree).length
    val names = (0 until dim).map("poly_" + _)
    val expected = xs.map(v => PolynomialExpansion.expand(v, degree).toSeq)
    val missing = (0 until dim).map(_ => 0.0)
    test(PolynomialExpansion("poly", degree), xs, names, expected, missing)
  }

  private val quantileGen = Gen.listOfN(100, Gen.posNum[Double])
  property("quantile") = Prop.forAll(quantileGen, Gen.oneOf(2, 4, 5)) { (xs, numBuckets) =>
    val m = new JTreeMap[Double, Int]()
    for (i <- 1 until numBuckets) {
      val offset = xs.size / numBuckets * i
      m.put(xs(offset), i - 1)
    }
    m.put(Double.PositiveInfinity, numBuckets - 1)
    val expected = xs.map { x =>
      (0 until numBuckets).map(i => if (i == m.higherEntry(x).getValue) 1.0 else 0.0)
    }
    val names = (0 until numBuckets).map("quantile_" + _)
    val missing = (0 until numBuckets).map(_ => 0.0)
    test(QuantileDiscretizer("quantile", numBuckets), xs, names, expected, missing)
    true
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
