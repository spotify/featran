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

import org.apache.spark.rdd.{RDD, RDDUtil}

import scala.reflect.ClassTag

package object spark {

  /**
   * [[CollectionType]] for extraction from Apache Spark `RDD` type.
   */
  implicit object SparkCollectionType extends CollectionType[RDD] {
    override def map[A, B: ClassTag](ma: RDD[A])(f: A => B): RDD[B] =
      ma.map(f)

    override def reduce[A](ma: RDD[A])(f: (A, A) => A): RDD[A] =
      ma.context.parallelize(Seq(ma.reduce(f)))(RDDUtil.classTag(ma))

    override def cross[A, B: ClassTag](ma: RDD[A])(mb: RDD[B]): RDD[(A, B)] = {
      val b = mb.first()
      ma.map((_, b))
    }

    override def pure[A, B: ClassTag](ma: RDD[A])(b: B): RDD[B] = ma.context.parallelize(Seq(b))
  }

}
