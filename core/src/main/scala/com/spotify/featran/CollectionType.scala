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

import simulacrum._

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Type class for collections to extract features from.
 * @tparam M collection type
 */
@typeclass trait CollectionType[M[_]] {
  def pure[A, B: ClassTag](ma: M[A])(a: B): M[B]

  def map[A, B: ClassTag](ma: M[A])(f: A => B): M[B]

  def reduce[A](ma: M[A])(f: (A, A) => A): M[A]

  def cross[A, B: ClassTag](ma: M[A])(mb: M[B]): M[(A, B)]
}

object CollectionType {
  implicit def scalaCollectionType[M[_] <: Traversable[_]](
    implicit cbf: CanBuildFrom[M[_], _, M[_]]): CollectionType[M] =
    new CollectionType[M] {
      override def map[A, B: ClassTag](ma: M[A])(f: A => B): M[B] = {
        val builder = cbf().asInstanceOf[mutable.Builder[B, M[B]]]
        ma.asInstanceOf[Seq[A]].foreach(a => builder += f(a))
        builder.result()
      }

      override def pure[A, B: ClassTag](ma: M[A])(b: B): M[B] = {
        val builder = cbf().asInstanceOf[mutable.Builder[B, M[B]]]
        builder += b
        builder.result()
      }

      override def reduce[A](ma: M[A])(f: (A, A) => A): M[A] = {
        val builder = cbf().asInstanceOf[mutable.Builder[A, M[A]]]
        if (ma.asInstanceOf[Seq[A]].nonEmpty) {
          builder += ma.asInstanceOf[Seq[A]].reduce(f)
        }
        builder.result()
      }

      override def cross[A, B: ClassTag](ma: M[A])(mb: M[B]): M[(A, B)] = {
        val builder = cbf().asInstanceOf[mutable.Builder[(A, B), M[(A, B)]]]
        val seq = mb.asInstanceOf[Seq[B]]
        if (seq.nonEmpty) {
          val b = mb.asInstanceOf[Seq[B]].head
          ma.asInstanceOf[Seq[A]].foreach(a => builder += ((a, b)))
        }
        builder.result()
      }
    }

  implicit val arrayCollectionType: CollectionType[Array] = new CollectionType[Array] {
    override def pure[A, B: ClassTag](ma: Array[A])(b: B): Array[B] = Array(b)

    override def map[A, B: ClassTag](ma: Array[A])(f: A => B): Array[B] =
      ma.map(f)

    override def reduce[A](ma: Array[A])(f: (A, A) => A): Array[A] = {
      // workaround for "No ClassTag available for A"
      val r = ma.take(1)
      r(0) = ma.reduce(f)
      r
    }
    override def cross[A, B: ClassTag](ma: Array[A])(mb: Array[B]): Array[(A, B)] =
      ma.map((_, mb.head))
  }
}
