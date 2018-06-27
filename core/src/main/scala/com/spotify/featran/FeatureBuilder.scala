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

import breeze.linalg.{DenseVector, SparseVector}
import breeze.math.Semiring
import breeze.storage.Zero
import com.spotify.featran.transformers.Transformer
import simulacrum.typeclass

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

sealed trait FeatureRejection
object FeatureRejection {
  case class OutOfBound(lower: Double, upper: Double, actual: Double) extends FeatureRejection
  case class Unseen(labels: Set[String]) extends FeatureRejection
  case class WrongDimension(expected: Int, actual: Int) extends FeatureRejection
  case class Outlier(actual: Double) extends FeatureRejection
  case object Collision extends FeatureRejection
}

/**
 * Type class for types to build feature into.
 * @tparam T output feature type
 */
@typeclass trait FeatureBuilder[T] extends Serializable { self =>

  private val _rejections: mutable.Map[String, FeatureRejection] =
    mutable.Map.empty

  /**
   * Reject an input row.
   * @param transformer transformer rejecting the input
   * @param reason reason for rejection
   */
  def reject(transformer: Transformer[_, _, _], reason: FeatureRejection): Unit = {
    val name = transformer.name
    require(!_rejections.contains(name), s"Transformer $name already rejected")
    _rejections(name) = reason
  }

  /**
   * Gather rejections. This should be called only once per input row.
   * @return
   */
  def rejections: Map[String, FeatureRejection] = {
    val r = _rejections.toMap
    _rejections.clear()
    r
  }

  /**
   * Initialize the builder for a record. This should be called only once per input row.
   * @param dimension total feature dimension
   */
  def init(dimension: Int): Unit

  /**
   * Prepare the builder for the next transformer. This should be called only once per transformer
   * per input row.
   * @param transformer the next transformer in line
   */
  def prepare(transformer: Transformer[_, _, _]): Unit = Unit

  /**
   * Gather builder result for a result. This should be called only once per input row.
   */
  def result: T

  /**
   * Add a single feature value. The total number of values added and skipped should equal to
   * dimension in [[init]].
   */
  def add(name: String, value: Double): Unit

  /**
   * Skip a single feature value. The total number of values added and skipped should equal to
   * dimension in [[init]].
   */
  def skip(): Unit

  /**
   * Add multiple feature values. The total number of values added and skipped should equal to
   * dimension in [[init]].
   */
  def add[M[_]](names: Iterable[String], values: M[Double])(
    implicit ev: M[Double] => Seq[Double]): Unit = {
    val i = names.iterator
    val j = values.iterator
    while (i.hasNext && j.hasNext) {
      add(i.next(), j.next())
    }
  }

  /**
   * Skip multiple feature values. The total number of values added and skipped should equal to
   * dimension in [[init]].
   */
  def skip(n: Int): Unit = {
    var i = 0
    while (i < n) {
      skip()
      i += 1
    }
  }

  /**
   * Create a [[FeatureBuilder]] for type `U` by converting the result type `T` with `f`.
   */
  def map[U](f: T => U): FeatureBuilder[U] = new FeatureBuilder[U] {
    private val delegate = self
    private val g = f
    override def init(dimension: Int): Unit = delegate.init(dimension)
    override def add(name: String, value: Double): Unit =
      delegate.add(name, value)
    override def skip(): Unit = delegate.skip()
    override def result: U = g(delegate.result)

    override def newBuilder: FeatureBuilder[U] = delegate.newBuilder.map(g)
  }

  def newBuilder: FeatureBuilder[T]
}

/**
 * A sparse representation of an array using two arrays for indices and values of non-zero entries.
 */
case class SparseArray[@specialized(Float, Double) T](indices: Array[Int],
                                                      values: Array[T],
                                                      length: Int) {
  def toDense(implicit ct: ClassTag[T]): Array[T] = {
    val r = new Array[T](length)
    var i = 0
    while (i < indices.length) {
      r(indices(i)) = values(i)
      i += 1
    }
    r
  }
}

/**
 * A [[SparseArray]] with names of non-zero entries.
 */
case class NamedSparseArray[@specialized(Float, Double) T](indices: Array[Int],
                                                           values: Array[T],
                                                           length: Int,
                                                           names: Seq[String]) {
  def toDense(implicit ct: ClassTag[T]): Array[T] = {
    val r = new Array[T](length)
    var i = 0
    while (i < indices.length) {
      r(indices(i)) = values(i)
      i += 1
    }
    r
  }
}

object FeatureBuilder {

  private final case class ArrayFB[T: ClassTag: FloatingPoint](
    private var underlying: Array[T] = null)
      extends FeatureBuilder[Array[T]] {
    private var offset: Int = 0

    override def init(dimension: Int): Unit = underlying = new Array[T](dimension)

    override def add(name: String, value: Double): Unit = {
      underlying(offset) = FloatingPoint[T].fromDouble(value)
      offset += 1
    }

    override def skip(): Unit = offset += 1

    override def skip(n: Int): Unit = offset += n

    override def result: Array[T] = {
      require(offset == underlying.length)
      offset = 0
      underlying.clone()
    }

    override def newBuilder: FeatureBuilder[Array[T]] = ArrayFB()
  }

  implicit def arrayFB[T: ClassTag: FloatingPoint]: FeatureBuilder[Array[T]] = ArrayFB()

  // Workaround for CanBuildFrom not serializable
  trait CanBuild[T, M] extends Serializable {
    def apply(): mutable.Builder[T, M]
  }

  private def newCB[T, M](f: () => mutable.Builder[T, M]) = new CanBuild[T, M] {
    override def apply(): mutable.Builder[T, M] = f()
  }

  // Collection types in _root_.scala.*
  //scalastyle:off public.methods.have.type
  implicit def traversableCB[T] = newCB(() => Traversable.newBuilder[T])

  implicit def iterableCB[T] = newCB(() => Iterable.newBuilder[T])

  implicit def seqCB[T] = newCB(() => Seq.newBuilder[T])

  implicit def indexedSeqCB[T] = newCB(() => IndexedSeq.newBuilder[T])

  implicit def listCB[T] = newCB(() => List.newBuilder[T])

  implicit def vectorCB[T] = newCB(() => Vector.newBuilder[T])

  //scalastyle:on public.methods.have.type

  private final case class TraversableFB[M[_] <: Traversable[_], T: ClassTag: FloatingPoint]()(
    implicit cb: CanBuild[T, M[T]])
      extends FeatureBuilder[M[T]] {
    private var underlying: mutable.Builder[T, M[T]] = null

    override def init(dimension: Int): Unit = underlying = cb()

    override def add(name: String, value: Double): Unit =
      underlying += FloatingPoint[T].fromDouble(value)

    override def skip(): Unit = underlying += FloatingPoint[T].fromDouble(0.0)

    override def result: M[T] = underlying.result()

    override def newBuilder: FeatureBuilder[M[T]] = TraversableFB[M, T]()
  }

  implicit def traversableFB[M[_] <: Traversable[_], T: ClassTag: FloatingPoint](
    implicit cb: CanBuild[T, M[T]]): FeatureBuilder[M[T]] = TraversableFB[M, T]()

  private final case class NamedSparseArrayFB[T: ClassTag: FloatingPoint](
    private val withNames: Boolean)
      extends FeatureBuilder[NamedSparseArray[T]] {
    private var indices: Array[Int] = null
    private var values: Array[T] = null
    private val names: mutable.Buffer[String] = mutable.Buffer.empty
    private val initCapacity = 1024
    private var dim: Int = _
    private var offset: Int = 0
    private var i: Int = 0

    override def init(dimension: Int): Unit = {
      dim = dimension
      offset = 0
      i = 0
      if (indices == null) {
        val n = math.min(dimension, initCapacity)
        indices = new Array[Int](n)
        values = new Array[T](n)
      }
    }

    override def add(name: String, value: Double): Unit = {
      indices(i) = offset
      values(i) = FloatingPoint[T].fromDouble(value)
      if (withNames) {
        names.append(name)
      }
      i += 1
      offset += 1
      if (indices.length == i) {
        val n = indices.length * 2
        val newIndices = new Array[Int](n)
        val newValues = new Array[T](n)
        Array.copy(indices, 0, newIndices, 0, indices.length)
        Array.copy(values, 0, newValues, 0, indices.length)
        indices = newIndices
        values = newValues
      }
    }

    override def skip(): Unit = offset += 1

    override def skip(n: Int): Unit = offset += n

    override def result: NamedSparseArray[T] = {
      require(offset == dim)
      val rIndices = new Array[Int](i)
      val rValues = new Array[T](i)
      Array.copy(indices, 0, rIndices, 0, i)
      Array.copy(values, 0, rValues, 0, i)
      NamedSparseArray(rIndices, rValues, dim, names)
    }

    override def newBuilder: FeatureBuilder[NamedSparseArray[T]] = NamedSparseArrayFB(withNames)
  }

  implicit def sparseArrayFB[@specialized(Float, Double) T: ClassTag: FloatingPoint]
    : FeatureBuilder[SparseArray[T]] =
    NamedSparseArrayFB(withNames = false).map(a => SparseArray(a.indices, a.values, a.length))

  implicit def namedSparseArrayFB[@specialized(Float, Double) T: ClassTag: FloatingPoint]
    : FeatureBuilder[NamedSparseArray[T]] = NamedSparseArrayFB(withNames = true)

  implicit def denseVectorFB[T: ClassTag: FloatingPoint]: FeatureBuilder[DenseVector[T]] =
    FeatureBuilder[Array[T]].map(DenseVector(_))

  implicit def sparseVectorFB[T: ClassTag: FloatingPoint: Semiring: Zero]
    : FeatureBuilder[SparseVector[T]] =
    FeatureBuilder[SparseArray[T]].map { a =>
      new SparseVector(a.indices, a.values, a.indices.length, a.length)
    }

  private final case class MapFB[T: ClassTag: FloatingPoint]()
      extends FeatureBuilder[Map[String, T]] { self =>
    private var underlying: java.util.Map[String, T] = null

    override def init(dimension: Int): Unit =
      underlying = new java.util.HashMap[String, T]

    override def add(name: String, value: Double): Unit =
      underlying.put(name, FloatingPoint[T].fromDouble(value))

    override def skip(): Unit = Unit

    override def skip(n: Int): Unit = Unit

    override def result: Map[String, T] = new JMapWrapper(underlying)

    override def newBuilder: FeatureBuilder[Map[String, T]] = MapFB()
  }

  implicit def mapFB[T: ClassTag: FloatingPoint]: FeatureBuilder[Map[String, T]] = MapFB()
}

private final class JMapWrapper[K, V](m: java.util.Map[K, V]) extends Map[K, V] with Serializable {
  // scalastyle:off method.name
  override def +[B1 >: V](kv: (K, B1)): Map[K, B1] = m.asScala.toMap + kv

  override def -(key: K): Map[K, V] = m.asScala.toMap - key
  // scalastyle:on method.name

  override def get(key: K): Option[V] = Option(m.get(key))

  override def iterator: Iterator[(K, V)] = m.asScala.iterator
}
