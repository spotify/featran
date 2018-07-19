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

import scala.reflect.ClassTag

/**
 * Encapsulate features extracted from a [[FeatureSpec]].
 * @tparam M input collection type, e.g. `Array`, List
 * @tparam T input record type to extract features from
 */
class FeatureExtractor[M[_]: CollectionType, T] private[featran] (
  private val fs: M[FeatureSet[T]],
  @transient private val input: M[T],
  @transient private val settings: Option[M[String]])
    extends Serializable {
  import FeatureSpec.ARRAY, CollectionType.ops._, json._

  @transient private[featran] lazy val as: M[(T, ARRAY)] =
    input.cross(fs).map { case (in, spec) => (in, spec.unsafeGet(in)) }

  @transient private[featran] lazy val aggregate: M[ARRAY] = settings match {
    case Some(x) =>
      x.cross(fs).map {
        case (s, spec) =>
          spec.decodeAggregators(decode[Seq[Settings]](s).right.get)
      }
    case None =>
      as.cross(fs)
        .map {
          case ((_, array), featureSet) =>
            (featureSet, featureSet.unsafePrepare(array))
        }
        .reduce {
          case ((featureSet, a), (_, b)) =>
            (featureSet, featureSet.unsafeSum(a, b))
        }
        .map { case (featureSet, array) => featureSet.unsafePresent(array) }
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
      aggregate.cross(fs).map {
        case (a, featureSet) =>
          encode[Seq[Settings]](featureSet.featureSettings(a)).noSpaces
      }
  }

  /**
   * Names of the extracted features, in the same order as values in [[featureValues]].
   */
  @transient lazy val featureNames: M[Seq[String]] =
    aggregate.cross(fs).map(x => x._2.featureNames(x._1))

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
    val fb = FeatureBuilder[F].newBuilder
    as.cross(aggregate).cross(fs).map {
      case (((o, a), c), spec) =>
        val cfb = CrossingFeatureBuilder(fb, spec.crossings)
        spec.featureValues(a, c, cfb)
        FeatureResult(cfb.result, cfb.rejections, o)
    }
  }
}

case class FeatureResult[F, T](value: F, rejections: Map[String, FeatureRejection], original: T)

object RecordExtractor {
  private class PipeIterator[T] extends Iterator[T] {
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

  private final case class State[F, T](input: PipeIterator[T],
                                       extractor: FeatureExtractor[Iterator, T],
                                       output: Iterator[FeatureResult[F, T]])
}

/** Encapsulate [[RecordExtractor]] for extracting individual records. */
class RecordExtractor[T, F: FeatureBuilder: ClassTag] private[featran] (fs: FeatureSet[T],
                                                                        settings: String) {
  import RecordExtractor._

  private implicit val iteratorCollectionType: CollectionType[Iterator] =
    new CollectionType[Iterator] {
      override def map[A, B: ClassTag](ma: Iterator[A])(f: A => B): Iterator[B] = ma.map(f)

      override def reduce[A](ma: Iterator[A])(f: (A, A) => A): Iterator[A] = ???

      override def cross[A, B: ClassTag](ma: Iterator[A])(mb: Iterator[B]): Iterator[(A, B)] = {
        val b = mb.next()
        ma.map(a => (a, b))
      }

      override def pure[A, B: ClassTag](ma: Iterator[A])(b: B): Iterator[B] = Iterator(b)
    }

  private val state = new ThreadLocal[State[F, T]] {
    override def initialValue(): State[F, T] = {
      val input: PipeIterator[T] = new PipeIterator[T]
      val extractor: FeatureExtractor[Iterator, T] =
        new FeatureExtractor[Iterator, T](Iterator.continually(fs),
                                          input,
                                          Some(Iterator.continually(settings)))
      val output: Iterator[FeatureResult[F, T]] = extractor.featureResults

      State(input, extractor, output)
    }
  }

  /**
   * JSON settings of the [[FeatureSpec]] and aggregated feature summary.
   *
   * This can be used with [[FeatureSpec.extractWithSettings[F]*]] to bypass the `reduce` step when
   * extracting new records of the same type.
   */
  val featureSettings: String = settings

  /** Names of the extracted features, in the same order as values in [[featureValue]]. */
  val featureNames: Seq[String] = state.get().extractor.featureNames.next()

  /**
   * Extract feature values from a single record with values in the same order as names in
   * [[featureNames]].
   */
  def featureValue(record: T): F = featureResult(record).value

  /**
   * Extract feature values from a single record, with values in the same order as names in
   * [[featureNames]] with rejections keyed on feature name and the original input record.
   */
  def featureResult(record: T): FeatureResult[F, T] = {
    val s = state.get()
    s.input.feed(record)
    s.output.next()
  }
}
