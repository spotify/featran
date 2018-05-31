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

import com.spotify.scio.values.SCollection

import scala.reflect.ClassTag

package object scio {

  /**
   * [[CollectionType]] for extraction from Scio `SCollection` type.
   */
  implicit object ScioCollectionType extends CollectionType[SCollection] {
    override def map[A, B: ClassTag](ma: SCollection[A])(f: A => B): SCollection[B] = ma.map(f)

    override def reduce[A](ma: SCollection[A])(f: (A, A) => A): SCollection[A] =
      ma.reduce(f)

    override def cross[A, B: ClassTag](ma: SCollection[A])(
      mb: SCollection[B]): SCollection[(A, B)] = ma.cross(mb)

    override def pure[A, B: ClassTag](ma: SCollection[A])(b: B): SCollection[B] =
      ma.context.parallelize(Seq(b))
  }

}
