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

package com.spotify.featran

import com.spotify.featran.transformers.Settings

import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

/**
 * Encapsulate features extracted from a [[FeatureSpec]].
 * @tparam M input collection type, e.g. `Array`, List
 * @tparam T input record type to extract features from
 */
class FeatureExtractor[M[_]: CollectionType, T] private[featran] (
  private val fs: FeatureSet[T],
  @transient private val input: M[T],
  @transient private val settings: Option[M[String]])
    extends Serializable {

  import FeatureSpec.ARRAY

  @transient private val dt: CollectionType[M] = implicitly[CollectionType[M]]
  import dt.Ops._

  @transient private[featran] lazy val as: M[(T, ARRAY)] = {
    val g = fs // defeat closure
    input.map(o => (o, g.unsafeGet(o)))
  }
  @transient private[featran] lazy val aggregate: M[ARRAY] = settings match {
    case Some(x) =>
      x.map { s =>
        import io.circe.generic.auto._
        import io.circe.parser._
        fs.decodeAggregators(decode[Seq[Settings]](s).right.get)
      }
    case None =>
      as.map(t => fs.unsafePrepare(t._2)).reduce(fs.unsafeSum).map(fs.unsafePresent)
  }

  /**
   * JSON settings of the [[FeatureSpec]] and aggregated feature summary.
   *
   * This can be used with [[FeatureSpec.extractWithSettings[F]*]] to bypass the `reduce` step when
   * extracting new records of the same type.
   */
  @transient lazy val featureSettings: M[String] = settings match {
    case Some(x) => x
    case None =>
      aggregate.map { a =>
        import io.circe.generic.auto._
        import io.circe.syntax._
        fs.featureSettings(a).asJson.noSpaces
      }
  }

  /**
   * Names of the extracted features, in the same order as values in [[featureValues]].
   */
  @transient lazy val featureNames: M[Seq[String]] =
    aggregate.map(fs.featureNames)

  /**
   * Values of the extracted features, in the same order as names in [[featureNames]].
   * @tparam F output data type, e.g. `Array[Float]`, `Array[Double]`, `DenseVector[Float]`,
   *           `DenseVector[Double]`
   */
  def featureValues[F: FeatureBuilder: ClassTag]: M[F] =
    featureResults.map(_.value)

  /**
   * Values of the extracted features, in the same order as names in [[featureNames]] with
   * rejections keyed on feature name and the original input record.
   * @tparam F output data type, e.g. `Array[Float]`, `Array[Double]`, `DenseVector[Float]`,
   *           `DenseVector[Double]`
   */
  def featureResults[F: FeatureBuilder: ClassTag]: M[FeatureResult[F, T]] = {
    val fb = CrossingFeatureBuilder(implicitly[FeatureBuilder[F]].newBuilder, fs.crossings)
    as.cross(aggregate).map {
      case ((o, a), c) =>
        fs.featureValues(a, c, fb)
        FeatureResult(fb.result, fb.rejections, o)
    }
  }

}

case class FeatureResult[F, T](value: F, rejections: Map[String, FeatureRejection], original: T)

/** Encapsulate [[RecordExtractor]] for extracting individual records. */
class RecordExtractor[T, F: FeatureBuilder: ClassTag] private[featran] (fs: FeatureSet[T],
                                                                        settings: String) {

  private implicit val iteratorCollectionType: CollectionType[Iterator] =
    new CollectionType[Iterator] {
      override def map[A, B: ClassTag](ma: Iterator[A], f: A => B): Iterator[B] = ma.map(f)
      override def reduce[A](ma: Iterator[A], f: (A, A) => A): Iterator[A] = ???
      override def cross[A, B: ClassTag](ma: Iterator[A], mb: Iterator[B]): Iterator[(A, B)] = {
        val b = mb.next()
        ma.map(a => (a, b))
      }
    }

  private final case class State(fs: FeatureSet[T], settings: String) {
    private val input: PipeIterator = new PipeIterator
    private val extractor: FeatureExtractor[Iterator, T] =
      new FeatureExtractor[Iterator, T](fs, input, Some(Iterator.continually(settings)))
    private val output: Iterator[FeatureResult[F, T]] = extractor.featureResults

    val featureNames: Seq[String] = extractor.featureNames.next()

    def featureResult(record: T): FeatureResult[F, T] = {
      input.feed(record)
      output.next()
    }
  }

  private val state = new ThreadLocal[State] {
    override def initialValue(): State = State(fs, settings)
  }

  /**
   * JSON settings of the [[FeatureSpec]] and aggregated feature summary.
   *
   * This can be used with [[FeatureSpec.extractWithSettings[F]*]] to bypass the `reduce` step when
   * extracting new records of the same type.
   */
  val featureSettings: String = settings

  /** Names of the extracted features, in the same order as values in [[featureValue]]. */
  val featureNames: Seq[String] = state.get().featureNames

  /**
   * Extract feature values from a single record with values in the same order as names in
   * [[featureNames]].
   */
  def featureValue(record: T): F = featureResult(record).value

  /**
   * Extract feature values from a single record, with values in the same order as names in
   * [[featureNames]] with rejections keyed on feature name and the original input record.
   */
  def featureResult(record: T): FeatureResult[F, T] = state.get().featureResult(record)

  private class PipeIterator extends Iterator[T] {
    private var element: T = _
    private var hasElement: Boolean = false
    def feed(element: T): Unit = {
      require(!hasNext)
      this.element = element
      hasElement = true
    }
    override def hasNext: Boolean = hasElement
    override def next(): T = {
      require(hasNext)
      hasElement = false
      element
    }
  }

}
