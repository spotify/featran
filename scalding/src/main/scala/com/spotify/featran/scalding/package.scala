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

import com.twitter.algebird.Semigroup
import com.twitter.scalding.typed.TypedPipe

import scala.reflect.ClassTag

package object scalding {

  /**
   * [[CollectionType]] for extraction from Scalding `TypedPipe` type.
   */
  implicit object ScaldingCollectionType extends CollectionType[TypedPipe] {
    override def map[A, B: ClassTag](ma: TypedPipe[A])(f: A => B): TypedPipe[B] = ma.map(f)

    override def reduce[A](ma: TypedPipe[A])(f: (A, A) => A): TypedPipe[A] =
      ma.sum(Semigroup.from(f))

    override def cross[A, B: ClassTag](ma: TypedPipe[A])(mb: TypedPipe[B]): TypedPipe[(A, B)] =
      ma.cross(mb)

    override def pure[A, B: ClassTag](ma: TypedPipe[A])(b: B): TypedPipe[B] =
      TypedPipe.from(Iterable(b))
  }

}
