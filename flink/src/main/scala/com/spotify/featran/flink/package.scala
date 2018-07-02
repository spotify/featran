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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet

import scala.reflect.ClassTag

package object flink {

  /**
   * [[CollectionType]] for extraction from Apache Flink `DataSet` type.
   */
  implicit object FlinkCollectionType extends CollectionType[DataSet] {
    // force fallback to default serializer
    private val Ti = TypeInformation.of(classOf[Any])

    override def map[A, B: ClassTag](ma: DataSet[A])(f: A => B): DataSet[B] = {
      implicit val tib = Ti.asInstanceOf[TypeInformation[B]]
      ma.map(f)
    }
    override def reduce[A](ma: DataSet[A])(f: (A, A) => A): DataSet[A] =
      ma.reduce(f)

    override def cross[A, B: ClassTag](ma: DataSet[A])(mb: DataSet[B]): DataSet[(A, B)] = {
      implicit val tib = Ti.asInstanceOf[TypeInformation[B]]
      ma.crossWithTiny(mb)
    }

    override def pure[A, B: ClassTag](ma: DataSet[A])(b: B): DataSet[B] = {
      implicit val tib = Ti.asInstanceOf[TypeInformation[B]]
      ma.getExecutionEnvironment.fromElements(b)
    }
  }

}
