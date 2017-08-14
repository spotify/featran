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

import scala.collection.mutable
import scala.language.higherKinds
import scala.reflect.ClassTag

/**
 * Type class for types to build feature into.
 * @tparam T output feature type
 */
trait FeatureBuilder[T] extends Serializable { self =>

  /**
   * Initialize the builder for a record. This should be called only once per input row.
   * @param dimension total feature dimension
   */
  def init(dimension: Int): Unit

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
  def add[M[_]](names: Iterator[String], values: M[Double])
               (implicit ev: M[Double] => Seq[Double]): Unit = {
    val i = values.iterator
    while (names.hasNext && i.hasNext) {
      add(names.next(), i.next())
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
    override def add(name: String, value: Double): Unit = delegate.add(name, value)
    override def skip(): Unit = delegate.skip()
    override def result: U = g(delegate.result)
  }

}

object FeatureBuilder {

  implicit def arrayFB[T: ClassTag : FloatingPoint]: FeatureBuilder[Array[T]] =
    new FeatureBuilder[Array[T]] {
      private var array: Array[T] = _
      private var offset: Int = 0
      private val fp = implicitly[FloatingPoint[T]]
      override def init(dimension: Int): Unit = array = new Array[T](dimension)
      override def add(name: String, value: Double): Unit = {
        array(offset) = fp.fromDouble(value)
        offset += 1
      }
      override def skip(): Unit = offset += 1
      override def skip(n: Int): Unit = offset += n
      override def result: Array[T] = {
        require(offset == array.length)
        offset = 0
        array.clone()
      }
    }

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

  implicit def traversableFB[M[_] <: Traversable[_], T: ClassTag : FloatingPoint]
  (implicit cb: CanBuild[T, M[T]]): FeatureBuilder[M[T]] = new FeatureBuilder[M[T]] {
    private var b: mutable.Builder[T, M[T]] = _
    private val fp = implicitly[FloatingPoint[T]]
    override def init(dimension: Int): Unit = b = cb()
    override def add(name: String, value: Double): Unit = b += fp.fromDouble(value)
    override def skip(): Unit = b += fp.fromDouble(0.0)
    override def result: M[T] = b.result()
  }

  implicit def denseVectorFB[T: ClassTag : FloatingPoint]: FeatureBuilder[DenseVector[T]] =
    implicitly[FeatureBuilder[Array[T]]].map(DenseVector(_))

  implicit def sparseVectorFB[T: ClassTag : FloatingPoint : Semiring : Zero]
  : FeatureBuilder[SparseVector[T]] = new FeatureBuilder[SparseVector[T]] {
    private var dim: Int = _
    private var offset: Int = 0
    private val queue: mutable.Queue[(Int, T)] = mutable.Queue.empty
    private val fp = implicitly[FloatingPoint[T]]
    override def init(dimension: Int): Unit = {
      dim = dimension
      offset = 0
      queue.clear()
    }
    override def add(name: String, value: Double): Unit = {
      queue.enqueue((offset, fp.fromDouble(value)))
      offset += 1

    }
    override def skip(): Unit = offset += 1
    override def skip(n: Int): Unit = offset += n
    override def result: SparseVector[T] = {
      require(offset == dim)
      SparseVector(dim)(queue: _*)
    }
  }

  implicit def mapFB[T: ClassTag : FloatingPoint]: FeatureBuilder[Map[String, T]] =
    new FeatureBuilder[Map[String, T]] { self =>
      private var b: mutable.Builder[(String, T), Map[String, T]] = _
      private val fp = implicitly[FloatingPoint[T]]
      override def init(dimension: Int): Unit = b = Map.newBuilder[String, T]
      override def add(name: String, value: Double): Unit = b += name -> fp.fromDouble(value)
      override def skip(): Unit = Unit
      override def skip(n: Int): Unit = Unit
      override def result: Map[String, T] = b.result()
    }

}
