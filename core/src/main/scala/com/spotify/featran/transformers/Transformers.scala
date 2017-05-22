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

  // Missing value = if (withMean) 0.0 else mean
  def standard(name: String,
               withStd: Boolean = true,
               withMean: Boolean = false): Transformer[Double, Moments, (Double, Double)] =
    new StandardScaler(name, withStd, withMean)

}

//================================================================================
// Utilities
//================================================================================

private abstract class OneDimensional[A, B, C](name: String) extends Transformer[A, B, C](name) {
  override def featureDimension(c: C): Int = 1
  override def featureNames(c: C): Seq[String] = Seq(name)
}

private abstract class MapOne[A](name: String, val default: Double = 0.0)
  extends OneDimensional[A, Unit, Unit](name) {
  override val aggregator: Aggregator[A, Unit, Unit] = Aggregators.unit[A]
  override def buildFeatures(a: Option[A], c: Unit, fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) => fb.add(map(x))
    case None => fb.add(default)
  }
  def map(a: A): Double
}

object Aggregators {
  def unit[A]: Aggregator[A, Unit, Unit] = from[A](_ => ()).to(_ => ())

  def from[A]: From[A] = new From[A]
  class From[A] extends Serializable {
    def apply[B: Semigroup](f: A => B): FromSemigroup[A, B] = new FromSemigroup[A, B](f)
  }
  class FromSemigroup[A, B: Semigroup](f: A => B) extends Serializable {
    def to[C](g: B => C): Aggregator[A, B, C] = new Aggregator[A, B, C] {
      override def prepare(input: A): B = f(input)
      override def semigroup: Semigroup[B] = implicitly[Semigroup[B]]
      override def present(reduction: B): C = g(reduction)
    }
  }
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
    Aggregators.from[Double](x => (Min(x), Max(x))).to(r => (r._1.get, r._2.get - r._1.get))
  override def buildFeatures(a: Option[Double], c: (Double, Double),
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) => fb.add((x - c._1) / c._2 * (max - min) + min)
    case None => fb.add(min)
  }
}

private class NHotEncoder(name: String)
  extends Transformer[Seq[String], Set[String], Array[String]](name) {
  override val aggregator: Aggregator[Seq[String], Set[String], Array[String]] =
    Aggregators.from[Seq[String]](_.toSet).to(_.toArray.sorted)
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
    Aggregators.from[String](Set(_)).to(_.toArray.sorted)
  override def featureDimension(c: Array[String]): Int = c.length
  override def featureNames(c: Array[String]): Seq[String] = c.map(name + "_" + _).toSeq
  override def buildFeatures(a: Option[String], c: Array[String], fb: FeatureBuilder[_]): Unit =
    c.foreach(s => if (a.contains(s)) fb.add(1.0) else fb.skip())
}

private class StandardScaler(name: String, val withStd: Boolean, val withMean: Boolean)
  extends Transformer[Double, Moments, (Double, Double)](name) {
  override val aggregator: Aggregator[Double, Moments, (Double, Double)] =
    Aggregators.from[Double](Moments(_)).to(r => (r.mean, r.stddev))
  override def featureDimension(c: (Double, Double)): Int = 1
  override def featureNames(c: (Double, Double)): Seq[String] = Seq(name)
  override def buildFeatures(a: Option[Double], c: (Double, Double),
                             fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) =>
      val r = (withStd, withMean) match {
        case (true, true) => (x - c._1) / c._2
        case (true, false) => (x - c._1) / c._2 + c._1
        case (false, true) => x - c._1
        case (false, false) => x
      }
      fb.add(r)
    case None => fb.add(if (withMean) 0.0 else c._1)
  }

}
