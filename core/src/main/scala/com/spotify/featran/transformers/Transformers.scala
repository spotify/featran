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

import com.spotify.featran.FeatureBuilder
import com.twitter.algebird._

// TODO: port more transformers from Spark
// https://spark.apache.org/docs/2.1.0/ml-features.html

trait Transformers {

  // Missing value = 0.0
  def binarizer(name: String, threshold: Double = 0.0): Transformer[Double, Unit, Unit] =
    new Binarizer(name, threshold)

  // Missing value = 0.0
  def identity(name: String): Transformer[Double, Unit, Unit] =
    new Identity(name)

  // Missing value = min
  def minMax(name: String, min: Double = 0.0, max: Double = 1.0)
  : Transformer[Double, (Min[Double], Max[Double]), (Double, Double)] =
    new MinMaxScaler(name, min, max)

  // Missing value = [0.0, 0.0, ...]
  def nHotEncoder(name: String): Transformer[Seq[String], Set[String], Array[String]] =
    new NHotEncoder(name)

  // Missing value = [0.0, 0.0, ...]
  def oneHotEncoder(name: String): Transformer[String, Set[String], Array[String]] =
    new OneHotEncoder(name)

}

//================================================================================
// Utilities
//================================================================================

private class UnitAggregator[T] extends Aggregator[T, Unit, Unit] {
  override def prepare(input: T): Unit = ()
  override def semigroup: Semigroup[Unit] = Semigroup.from[Unit] { case(_, _) => () }
  override def present(reduction: Unit): Unit = ()
}

private abstract class OneDimensional[A, B, C](name: String) extends Transformer[A, B, C](name) {
  override def featureDimension(c: C): Int = 1
  override def featureNames(c: C): Seq[String] = Seq(name)
}

private abstract class MapOne[A](name: String, val default: Double = 0.0)
  extends OneDimensional[A, Unit, Unit](name) {
  override val aggregator: Aggregator[A, Unit, Unit] = new UnitAggregator[A]
  override def buildFeatures(a: Option[A], c: Unit, fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) => fb.add(map(x))
    case None => fb.add(default)
  }
  def map(a: A): Double
}

//================================================================================
// Transformers
//================================================================================

private class Binarizer(name: String, val threshold: Double) extends MapOne[Double](name) {
  override def map(a: Double): Double = if (a > threshold) 1.0 else 0.0
}

private class Identity(name: String) extends MapOne[Double](name) {
  override def map(a: Double): Double = a
}

private class MinMaxScaler(name: String, val min: Double, val max: Double)
  extends OneDimensional[Double, (Min[Double], Max[Double]), (Double, Double)](name) {
  override val aggregator: Aggregator[Double, (Min[Double], Max[Double]), (Double, Double)] =
    new Aggregator[Double, (Min[Double], Max[Double]), (Double, Double)] {
      override def prepare(input: Double): (Min[Double], Max[Double]) = (Min(input), Max(input))
      override def semigroup: Semigroup[(Min[Double], Max[Double])] =
        implicitly[Semigroup[(Min[Double], Max[Double])]]
      override def present(reduction: (Min[Double], Max[Double])): (Double, Double) =
        (reduction._1.get, reduction._2.get - reduction._1.get)
    }
  override def buildFeatures(a: Option[Double], c: (Double, Double),
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) => fb.add((x - c._1) / c._2 * (max - min) + min)
    case None => fb.add(min)
  }
}

private class NHotEncoder(name: String)
  extends Transformer[Seq[String], Set[String], Array[String]](name) {
  override val aggregator: Aggregator[Seq[String], Set[String], Array[String]] =
    new Aggregator[Seq[String], Set[String], Array[String]] {
      override def prepare(input: Seq[String]): Set[String] = input.toSet
      override def semigroup: Semigroup[Set[String]] = Semigroup.from(_ ++ _)
      override def present(reduction: Set[String]): Array[String] = reduction.toArray.sorted
    }
  override def featureDimension(c: Array[String]): Int = c.length
  override def featureNames(c: Array[String]): Seq[String] = c.map(name + "_" + _).toSeq
  override def buildFeatures(a: Option[Seq[String]], c: Array[String],
                             fb: FeatureBuilder[_]): Unit = {
    val as = a.toSet.flatten
    c.foreach(s => if (as.contains(s)) fb.add(1.0) else fb.skip())
  }
}

private class OneHotEncoder(name: String)
  extends Transformer[String, Set[String], Array[String]](name) {
  override val aggregator: Aggregator[String, Set[String], Array[String]] =
    new Aggregator[String, Set[String], Array[String]] {
      override def prepare(input: String): Set[String] = Set(input)
      override def semigroup: Semigroup[Set[String]] = Semigroup.from(_ ++ _)
      override def present(reduction: Set[String]): Array[String] = reduction.toArray.sorted
    }
  override def featureDimension(c: Array[String]): Int = c.length
  override def featureNames(c: Array[String]): Seq[String] = c.map(name + "_" + _).toSeq
  override def buildFeatures(a: Option[String], c: Array[String], fb: FeatureBuilder[_]): Unit =
    c.foreach(s => if (a.contains(s)) fb.add(1.0) else fb.skip())
}
