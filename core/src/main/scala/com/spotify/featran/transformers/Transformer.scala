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

import com.spotify.featran.{FeatureBuilder, FlatReader, FlatWriter}
import com.twitter.algebird.{Aggregator, Semigroup}

trait SettingsBuilder {
  def fromSettings(settings: Settings): Transformer[_, _, _]
}

// TODO: port more transformers from Spark
// https://spark.apache.org/docs/2.1.0/ml-features.html

/**
 * Base class for feature transformers.
 *
 * Input values are converted into intermediate type `B`, aggregated, and converted to summary type
 * `C`. The summary type `C` is then used to transform input values into features.
 * @param name feature name
 * @tparam A input type
 * @tparam B aggregator intermediate type
 * @tparam C aggregator summary type
 */
abstract class Transformer[-A, B, C](val name: String) extends Serializable {

  require(name != null && name.nonEmpty, "name cannot be null or empty")

  protected def checkRange(name: String, value: Double, lower: Double, upper: Double): Unit =
    require(value >= lower && value <= upper, s"$name $value not in the range[$lower, $upper]")

  /**
   * Aggregator for computing input values into a summary.
   */
  val aggregator: Aggregator[A, B, C]

  /**
   * Number of generated features given an aggregator summary.
   */
  def featureDimension(c: C): Int

  /**
   * Names of the generated features given an aggregator summary.
   */
  def featureNames(c: C): Seq[String]

  /**
   * Build features from a single input value and an aggregator summary.
   * @param a input value
   * @param c aggregator summary
   * @param fb feature builder
   */
  def buildFeatures(a: Option[A], c: C, fb: FeatureBuilder[_]): Unit

  protected def nameAt(n: Int): String = name + '_' + n
  protected def names(n: Int): Stream[String] = (0 until n).toStream.map(nameAt)

  //================================================================================
  // Special cases when value is missing in all rows
  //================================================================================

  def optFeatureDimension(c: Option[C]): Int = c match {
    case Some(x) => featureDimension(x)
    case None    => 1
  }

  def optFeatureNames(c: Option[C]): Seq[String] = c match {
    case Some(x) => featureNames(x)
    case None    => Seq(name)
  }

  def optBuildFeatures(a: Option[A], c: Option[C], fb: FeatureBuilder[_]): Unit = c match {
    case Some(x) => buildFeatures(a, x, fb)
    case None    => fb.skip()
  }

  def unsafeBuildFeatures(a: Option[Any], c: Option[Any], fb: FeatureBuilder[_]): Unit =
    optBuildFeatures(a.asInstanceOf[Option[A]], c.asInstanceOf[Option[C]], fb)

  def unsafeFeatureDimension(c: Option[Any]): Int =
    optFeatureDimension(c.asInstanceOf[Option[C]])

  def flatRead[T: FlatReader]: T => Option[Any]
  def flatWriter[T](implicit fw: FlatWriter[T]): Option[A] => fw.IF
  def unsafeFlatWriter[T](implicit fw: FlatWriter[T]): Option[Any] => fw.IF =
    (o: Option[Any]) => flatWriter.apply(o.asInstanceOf[Option[A]]).asInstanceOf[fw.IF]

  //================================================================================
  // Transformer parameter and aggregator persistence
  //================================================================================

  /**
   * Encode aggregator summary of the current extraction.
   */
  def encodeAggregator(c: C): String

  /**
   * Decode aggregator summary from a previous extraction.
   */
  def decodeAggregator(s: String): C

  /**
   * Compile time parameters.
   */
  def params: Map[String, String] = Map.empty

  /**
   * Settings including compile time parameters and runtime aggregator summary.
   * @param c aggregator summary
   */
  def settings(c: Option[C]): Settings =
    Settings(this.getClass.getCanonicalName,
             name,
             params,
             optFeatureNames(c),
             c.map(encodeAggregator))

}

case class Settings(cls: String,
                    name: String,
                    params: Map[String, String],
                    featureNames: Seq[String],
                    aggregators: Option[String])

private[featran] abstract class OneDimensional[A, B, C](name: String)
    extends Transformer[A, B, C](name) {
  override def featureDimension(c: C): Int = 1
  override def featureNames(c: C): Seq[String] = Seq(name)
}

private[featran] abstract class MapOne[A](name: String)
    extends OneDimensional[A, Unit, Unit](name) {
  override val aggregator: Aggregator[A, Unit, Unit] = Aggregators.unit[A]
  override def buildFeatures(a: Option[A], c: Unit, fb: FeatureBuilder[_]): Unit = a match {
    case Some(x) => fb.add(name, map(x))
    case None    => fb.add(name, 0.0)
  }
  def map(a: A): Double
  override def encodeAggregator(c: Unit): String = ""
  override def decodeAggregator(s: String): Unit = ()
}

private object Aggregators {
  def unit[A]: Aggregator[A, Unit, Unit] = from[A](_ => ()).to(_ => ())

  def from[A]: From[A] = new From[A]
  class From[A] extends Serializable {
    def apply[B: Semigroup](f: A => B): FromSemigroup[A, B] =
      new FromSemigroup[A, B](f)
  }
  class FromSemigroup[A, B: Semigroup](f: A => B) extends Serializable {
    def to[C](g: B => C): Aggregator[A, B, C] = new Aggregator[A, B, C] {
      override def prepare(input: A): B = f(input)
      override def semigroup: Semigroup[B] = implicitly[Semigroup[B]]
      override def present(reduction: B): C = g(reduction)
    }
  }

  def seqLength[T, M[_]](expectedLength: Int = 0)(
    implicit ev: M[T] => Seq[T]): Aggregator[M[T], Int, Int] =
    new Aggregator[M[T], Int, Int] {
      override def prepare(input: M[T]): Int = {
        if (expectedLength > 0) {
          require(input.length == expectedLength,
                  s"Invalid input length, expected: $expectedLength, actual: ${input.length}")
        }
        input.length
      }
      override def semigroup: Semigroup[Int] = Semigroup.from { (x, y) =>
        require(x == y, s"Different input lengths, $x != $y")
        x
      }
      override def present(reduction: Int): Int = reduction
    }

}
