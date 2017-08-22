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

package com.spotify.featran.java

import java.util.{Collections, Optional, List => JList}

import com.spotify.featran.{CollectionType, FeatureExtractor, FeatureSpec}

import scala.collection.JavaConverters._
import scala.language.higherKinds
import scala.reflect.ClassTag

private object JavaOps {

  def requiredFn[I, O](f: SerializableFunction[I, O]): I => O = (input: I) => f(input)

  def optionalFn[I, O](f: SerializableFunction[I, Optional[O]]): I => Option[O] =
    (input: I) => {
      val o = f(input)
      if (o.isPresent) Some(o.get()) else None
    }

  implicit val jListCollectionType = new CollectionType[JList] {
    override def map[A, B: ClassTag](ma: JList[A], f: A => B) = ma.asScala.map(f).asJava
    override def reduce[A](ma: JList[A], f: (A, A) => A) =
      Collections.singletonList(ma.asScala.reduce(f))
    override def cross[A, B: ClassTag](ma: JList[A], mb: JList[B]) =
      ma.asScala.map((_, mb.get(0))).asJava
  }

  //================================================================================
  // Wrappers for FeatureSpec
  //================================================================================

  def extract[T](fs: FeatureSpec[T], input: JList[T]): FeatureExtractor[JList, T] =
    fs.extract(input)

  def extractWithSettings[T](fs: FeatureSpec[T], input: JList[T], settings: String)
  : FeatureExtractor[JList, T] =
    fs.extractWithSettings(input, Collections.singletonList(settings))

  //================================================================================
  // Wrappers for FeatureExtractor
  //================================================================================

  def featureSettings[T](fe: FeatureExtractor[JList, T]): String = fe.featureSettings.get(0)
  def featureNames[T](fe: FeatureExtractor[JList, T]): JList[String] = fe.featureNames.get(0).asJava
  def featureValuesFloat[T](fe: FeatureExtractor[JList, T]): JList[Array[Float]] =
    fe.featureValues[Array[Float]]
  def featureValuesDouble[T](fe: FeatureExtractor[JList, T]): JList[Array[Double]] =
    fe.featureValues[Array[Double]]

}
