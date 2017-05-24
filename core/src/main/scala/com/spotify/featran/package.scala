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

package com.spotify

import breeze.linalg.{DenseVector, SparseVector}
import breeze.math.Semiring
import breeze.storage.Zero

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.language.higherKinds
import scala.reflect.ClassTag

package object featran {

  implicit def scalaCollectionType[M[_] <: Traversable[_]]
  (implicit cbf: CanBuildFrom[M[_], _, M[_]]): CollectionType[M] = new CollectionType[M] {
    override def map[A, B: ClassTag](ma: M[A], f: (A) => B): M[B] = {
      val builder = cbf().asInstanceOf[mutable.Builder[B, M[B]]]
      ma.asInstanceOf[Seq[A]].foreach(a => builder += f(a))
      builder.result()
    }
    override def reduce[A](ma: M[A], f: (A, A) => A): M[A] = {
      val builder = cbf().asInstanceOf[mutable.Builder[A, M[A]]]
      builder += ma.asInstanceOf[Seq[A]].reduce(f)
      builder.result()
    }

    override def cross[A, B: ClassTag](ma: M[A], mb: M[B]): M[(A, B)] = {
      val builder = cbf().asInstanceOf[mutable.Builder[(A, B), M[(A, B)]]]
      val b = mb.asInstanceOf[Seq[B]].head
      ma.asInstanceOf[Seq[A]].foreach(a => builder += ((a, b)))
      builder.result()
    }
  }

  //================================================================================
  // Type class to generalize Float and Double
  //================================================================================

  trait FloatingPoint[@specialized (Float, Double) T] {
    def fromDouble(x: Double): T
  }
  implicit val floatFP: FloatingPoint[Float] = new FloatingPoint[Float] {
    override def fromDouble(x: Double): Float = x.toFloat
  }
  implicit val doubleFP: FloatingPoint[Double] = new FloatingPoint[Double] {
    override def fromDouble(x: Double): Double = x
  }

  //================================================================================
  // FeatureBuilder implementations
  //================================================================================

  implicit def arrayFB[T: ClassTag : FloatingPoint]: FeatureBuilder[Array[T]] =
    new FeatureBuilder[Array[T]] {
      private var array: Array[T] = _
      private var offset: Int = 0
      private val fp = implicitly[FloatingPoint[T]]
      override def init(dimension: Int): Unit = array = new Array[T](dimension)
      override def add(value: Double): Unit = {
        array(offset) = fp.fromDouble(value)
        offset += 1
      }
      override def skip(): Unit = offset += 1
      override def result: Array[T] = {
        require(offset == array.length)
        offset = 0
        array.clone()
      }
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
    override def add(value: Double): Unit = {
      queue.enqueue((offset, fp.fromDouble(value)))
      offset += 1

    }
    override def skip(): Unit = offset += 1
    override def result: SparseVector[T] = SparseVector(dim)(queue: _*)
  }

}
