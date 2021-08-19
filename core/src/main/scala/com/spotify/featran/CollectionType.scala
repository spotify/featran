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

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.annotation.implicitNotFound

/**
 * Type class for collections to extract features from.
 * @tparam M
 *   collection type
 */
@implicitNotFound("Could not find an instance of CollectionType for ${M}")
@typeclass trait CollectionType[M[_]] extends Serializable {
  def pure[A, B: ClassTag](ma: M[A])(a: B): M[B]

  def map[A, B: ClassTag](ma: M[A])(f: A => B): M[B]

  def reduce[A](ma: M[A])(f: (A, A) => A): M[A]

  def cross[A, B: ClassTag](ma: M[A])(mb: M[B]): M[(A, B)]
}

object CollectionType {
  implicit def scalaCollectionType[M[_]](implicit
    cb: CanBuild[_, M],
    ti: M[_] => Iterable[_]
  ): CollectionType[M] =
    new CollectionType[M] {
      override def map[A, B: ClassTag](ma: M[A])(f: A => B): M[B] = {
        val builder = cb().asInstanceOf[mutable.Builder[B, M[B]]]
        ma.asInstanceOf[Iterable[A]].foreach(a => builder += f(a))
        builder.result()
      }

      override def pure[A, B: ClassTag](ma: M[A])(b: B): M[B] = {
        val builder = cb().asInstanceOf[mutable.Builder[B, M[B]]]
        builder += b
        builder.result()
      }

      override def reduce[A](ma: M[A])(f: (A, A) => A): M[A] = {
        val builder = cb().asInstanceOf[mutable.Builder[A, M[A]]]
        if (ma.nonEmpty) {
          builder += ma.asInstanceOf[Iterable[A]].reduce(f)
        }
        builder.result()
      }

      override def cross[A, B: ClassTag](ma: M[A])(mb: M[B]): M[(A, B)] = {
        val builder = cb().asInstanceOf[mutable.Builder[(A, B), M[(A, B)]]]
        if (mb.nonEmpty) {
          val b = mb.asInstanceOf[Iterable[B]].head
          ma.asInstanceOf[Iterable[A]].foreach(a => builder += ((a, b)))
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

  /* ======================================================================== */
  /* THE FOLLOWING CODE IS MANAGED BY SIMULACRUM; PLEASE DO NOT EDIT!!!!      */
  /* ======================================================================== */

  /** Summon an instance of [[CollectionType]] for `M`. */
  @inline def apply[M[_]](implicit instance: CollectionType[M]): CollectionType[M] = instance

  object ops {
    implicit def toAllCollectionTypeOps[M[_], A](
      target: M[A]
    )(implicit tc: CollectionType[M]): AllOps[M, A] {
      type TypeClassType = CollectionType[M]
    } = new AllOps[M, A] {
      type TypeClassType = CollectionType[M]
      val self: M[A] = target
      val typeClassInstance: TypeClassType = tc
    }
  }
  trait Ops[M[_], A] extends Serializable {
    type TypeClassType <: CollectionType[M]
    def self: M[A]
    val typeClassInstance: TypeClassType
    def pure[B](a: B)(implicit ev$1: ClassTag[B]): M[B] = typeClassInstance.pure[A, B](self)(a)
    def map[B](f: A => B)(implicit ev$1: ClassTag[B]): M[B] = typeClassInstance.map[A, B](self)(f)
    def reduce(f: (A, A) => A): M[A] = typeClassInstance.reduce[A](self)(f)
    def cross[B](mb: M[B])(implicit ev$1: ClassTag[B]): M[(A, B)] =
      typeClassInstance.cross[A, B](self)(mb)
  }
  trait AllOps[M[_], A] extends Ops[M, A]
  trait ToCollectionTypeOps extends Serializable {
    implicit def toCollectionTypeOps[M[_], A](
      target: M[A]
    )(implicit tc: CollectionType[M]): Ops[M, A] {
      type TypeClassType = CollectionType[M]
    } = new Ops[M, A] {
      type TypeClassType = CollectionType[M]
      val self: M[A] = target
      val typeClassInstance: TypeClassType = tc
    }
  }
  object nonInheritedOps extends ToCollectionTypeOps

  /* ======================================================================== */
  /* END OF SIMULACRUM-MANAGED CODE                                           */
  /* ======================================================================== */

}
