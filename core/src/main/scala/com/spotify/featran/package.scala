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

import scala.collection.mutable
import scala.reflect.ClassTag

package object featran {

  object Transformers extends transformers.Transformers

  implicit object SeqCollectionType extends CollectionType[Seq] {
    override def map[A, B: ClassTag](ma: Seq[A], f: (A) => B): Seq[B] = ma.map(f)
    override def reduce[A](ma: Seq[A], f: (A, A) => A): Seq[A] = Seq(ma.reduce(f))
    override def cross[A, B: ClassTag](ma: Seq[A], mb: Seq[B]): Seq[(A, B)] = ma.map((_, mb.head))
  }

  implicit def faFeatureBuilder: FeatureBuilder[Array[Float]] =
    new FeatureBuilder[Array[Float]] {
      private var array: Array[Float] = _
      private var offset: Int = 0
      override def init(dimension: Int): Unit = array = new Array[Float](dimension)
      override def add(value: Double): Unit = {
        array(offset) = value.toFloat
        offset += 1
      }
      def skip(): Unit = offset += 1
      override def result: Array[Float] = {
        require(offset == array.length)
        offset = 0
        array.clone()
      }
    }

  implicit def daFeatureBuilder: FeatureBuilder[Array[Double]] =
    new FeatureBuilder[Array[Double]] {
      private var array: Array[Double] = _
      private var offset: Int = 0
      override def init(dimension: Int): Unit = {
        array = new Array[Double](dimension)
        offset = 0
      }
      override def add(value: Double): Unit = {
        array(offset) = value
        offset += 1
      }
      def skip(): Unit = offset += 1
      override def result: Array[Double] = {
        require(offset == array.length)
        array.clone()
      }
    }

  implicit def fdvFeatureBuilder: FeatureBuilder[DenseVector[Float]] =
    faFeatureBuilder.map(DenseVector(_))

  implicit def ddvFeatureBuilder: FeatureBuilder[DenseVector[Double]] =
    daFeatureBuilder.map(DenseVector(_))

  implicit def fsvFeatureBuilder: FeatureBuilder[SparseVector[Float]] =
    new FeatureBuilder[SparseVector[Float]] {
      private var dim: Int = _
      private var offset: Int = 0
      private val queue: mutable.Queue[(Int, Float)] = mutable.Queue.empty
      override def init(dimension: Int): Unit = {
        dim = dimension
        offset = 0
        queue.clear()
      }
      override def add(value: Double): Unit = {
        queue.enqueue((offset, value.toFloat))
        offset += 1

      }
      override def skip(): Unit = offset += 1
      override def result: SparseVector[Float] = SparseVector(dim)(queue: _*)
    }

  implicit def dsvFeatureBuilder: FeatureBuilder[SparseVector[Double]] =
    new FeatureBuilder[SparseVector[Double]] {
      private var dim: Int = _
      private var offset: Int = 0
      private val queue: mutable.Queue[(Int, Double)] = mutable.Queue.empty
      override def init(dimension: Int): Unit = {
        dim = dimension
        offset = 0
        queue.clear()
      }
      override def add(value: Double): Unit = {
        queue.enqueue((offset, value))
        offset += 1

      }
      override def skip(): Unit = offset += 1
      override def result: SparseVector[Double] = SparseVector(dim)(queue: _*)
    }

}
