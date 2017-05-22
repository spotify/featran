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
import com.twitter.algebird.{Aggregator, Semigroup}

// TODO: port more transformers from Spark
// https://spark.apache.org/docs/2.1.0/ml-features.html

abstract class Transformer[A, B, C](val name: String) extends Serializable {

  val aggregator: Aggregator[A, B, C]

  // number of generated features
  def featureDimension(c: C): Int

  // names of the generated features
  def featureNames(c: C): Seq[String]

  // build features
  def buildFeatures(a: Option[A], c: C, fb: FeatureBuilder[_]): Unit

  //================================================================================
  // Special cases when value is missing in all rows
  //================================================================================

  def optFeatureDimension(c: Option[C]): Int = c match {
    case Some(x) => featureDimension(x)
    case None => 1
  }

  def optFeatureNames(c: Option[C]): Seq[String] = c match {
    case Some(x) => featureNames(x)
    case None => Seq(name)
  }

  def optBuildFeatures(a: Option[A], c: Option[C], fb: FeatureBuilder[_]): Unit = c match {
    case Some(x) => buildFeatures(a, x, fb)
    case None => fb.skip()
  }

}

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

  val arrayLength: Aggregator[Array[Double], Int, Int] = new Aggregator[Array[Double], Int, Int] {
    override def prepare(input: Array[Double]): Int = input.length
    override def semigroup: Semigroup[Int] = Semigroup.from { (x, y) =>
      require(x == y)
      x
    }
    override def present(reduction: Int): Int = reduction
  }

}
