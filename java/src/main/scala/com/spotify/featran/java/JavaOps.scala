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

import java.lang.{Double => JDouble}
import java.util.function.BiFunction
import java.util.{Collections, Optional, List => JList}

import com.spotify.featran._
import com.spotify.featran.tensorflow._
import com.spotify.featran.xgboost._
import ml.dmlc.xgboost4j.LabeledPoint
import org.tensorflow.example.Example

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

// scalastyle:off number.of.methods
private object JavaOps {

  def requiredFn[I, O](f: SerializableFunction[I, O]): I => O =
    (input: I) => f(input)

  def optionalFn[I, O](f: SerializableFunction[I, Optional[O]]): I => Option[O] =
    (input: I) => {
      val o = f(input)
      if (o.isPresent) Some(o.get()) else None
    }

  def crossFn(f: BiFunction[JDouble, JDouble, JDouble]): (Double, Double) => Double =
    (a, b) => f(a, b)

  implicit val jListCollectionType: CollectionType[JList] = new CollectionType[JList] {
    override def map[A, B: ClassTag](ma: JList[A])(f: A => B): JList[B] =
      ma.asScala.map(f).asJava

    override def reduce[A](ma: JList[A])(f: (A, A) => A): JList[A] =
      Collections.singletonList(ma.asScala.reduce(f))

    override def cross[A, B: ClassTag](ma: JList[A])(mb: JList[B]): JList[(A, B)] =
      ma.asScala.map((_, mb.get(0))).asJava

    override def pure[A, B: ClassTag](ma: JList[A])(b: B): JList[B] = Collections.singletonList(b)
  }

  //================================================================================
  // Wrappers for FeatureSpec
  //================================================================================

  def extract[T](fs: FeatureSpec[T], input: JList[T]): FeatureExtractor[JList, T] =
    fs.extract(input)

  def extractWithSettings[T](fs: FeatureSpec[T],
                             input: JList[T],
                             settings: String): FeatureExtractor[JList, T] =
    fs.extractWithSettings(input, Collections.singletonList(settings))

  def extractWithSettingsFloat[T](fs: FeatureSpec[T],
                                  settings: String): RecordExtractor[T, Array[Float]] =
    fs.extractWithSettings(settings)
  def extractWithSettingsDouble[T](fs: FeatureSpec[T],
                                   settings: String): RecordExtractor[T, Array[Double]] =
    fs.extractWithSettings(settings)

  def extractWithSettingsFloatSparseArray[T](
    fs: FeatureSpec[T],
    settings: String): RecordExtractor[T, FloatSparseArray] =
    fs.extractWithSettings(settings)
  def extractWithSettingsDoubleSparseArray[T](
    fs: FeatureSpec[T],
    settings: String): RecordExtractor[T, DoubleSparseArray] =
    fs.extractWithSettings(settings)

  def extractWithSettingsFloatNamedSparseArray[T](
    fs: FeatureSpec[T],
    settings: String): RecordExtractor[T, FloatNamedSparseArray] =
    fs.extractWithSettings(settings)
  def extractWithSettingsDoubleNamedSparseArray[T](
    fs: FeatureSpec[T],
    settings: String): RecordExtractor[T, DoubleNamedSparseArray] =
    fs.extractWithSettings(settings)

  def extractWithSettingsExample[T](fs: FeatureSpec[T],
                                    settings: String): RecordExtractor[T, Example] =
    fs.extractWithSettings(settings)

  def extractWithSettingsLabeledPoint[T](fs: FeatureSpec[T],
                                         settings: String): RecordExtractor[T, LabeledPoint] =
    fs.extractWithSettings(settings)

  def extractWithSettingsSparseLabeledPoint[T](
    fs: FeatureSpec[T],
    settings: String): RecordExtractor[T, SparseLabeledPoint] =
    fs.extractWithSettings(settings)

  //================================================================================
  // Wrappers for FeatureExtractor
  //================================================================================

  def featureSettings[T](fe: FeatureExtractor[JList, T]): String =
    fe.featureSettings.get(0)
  def featureNames[T](fe: FeatureExtractor[JList, T]): JList[String] =
    fe.featureNames.get(0).asJava
  def featureValuesFloat[T](fe: FeatureExtractor[JList, T]): JList[Array[Float]] =
    fe.featureValues[Array[Float]]
  def featureValuesDouble[T](fe: FeatureExtractor[JList, T]): JList[Array[Double]] =
    fe.featureValues[Array[Double]]

  implicit def floatSparseArrayFB: FeatureBuilder[FloatSparseArray] =
    FeatureBuilder[SparseArray[Float]].map(a => new FloatSparseArray(a.indices, a.values, a.length))
  implicit def doubleSparseArrayFB: FeatureBuilder[DoubleSparseArray] =
    FeatureBuilder[SparseArray[Double]].map(a =>
      new DoubleSparseArray(a.indices, a.values, a.length))

  def featureValuesFloatSparseArray[T](fe: FeatureExtractor[JList, T]): JList[FloatSparseArray] =
    fe.featureValues[FloatSparseArray]
  def featureValuesDoubleSparseArray[T](fe: FeatureExtractor[JList, T]): JList[DoubleSparseArray] =
    fe.featureValues[DoubleSparseArray]

  implicit def floatNamedSparseArrayFB: FeatureBuilder[FloatNamedSparseArray] =
    FeatureBuilder[NamedSparseArray[Float]].map(a =>
      new FloatNamedSparseArray(a.indices, a.values, a.length, a.names))
  implicit def doubleNamedSparseArrayFB: FeatureBuilder[DoubleNamedSparseArray] =
    FeatureBuilder[NamedSparseArray[Double]].map(a =>
      new DoubleNamedSparseArray(a.indices, a.values, a.length, a.names))

  def featureValuesFloatNamedSparseArray[T](
    fe: FeatureExtractor[JList, T]): JList[FloatNamedSparseArray] =
    fe.featureValues[FloatNamedSparseArray]
  def featureValuesDoubleNamedSparseArray[T](
    fe: FeatureExtractor[JList, T]): JList[DoubleNamedSparseArray] =
    fe.featureValues[DoubleNamedSparseArray]

  def featureValuesExample[T](fe: FeatureExtractor[JList, T]): JList[Example] =
    fe.featureValues[Example]

  def featureValuesLabeledPoint[T](fe: FeatureExtractor[JList, T]): JList[LabeledPoint] =
    fe.featureValues[LabeledPoint]

  def featureValuesSparseLabeledPoint[T](
    fe: FeatureExtractor[JList, T]): JList[SparseLabeledPoint] =
    fe.featureValues[SparseLabeledPoint]

  //================================================================================
  // Wrappers for RecordExtractor
  //================================================================================

  def featureNames[F, T](fe: RecordExtractor[T, F]): JList[String] =
    fe.featureNames.asJava

}
// scalastyle:on number.of.methods

/** A sparse array of float values. */
class FloatSparseArray private[java] (indices: Array[Int],
                                      override val values: Array[Float],
                                      length: Int)
    extends SparseArray[Float](indices, values, length) {
  def toDense: Array[Float] = super.toDense
}

/** A sparse array of double values. */
class DoubleSparseArray private[java] (indices: Array[Int],
                                       override val values: Array[Double],
                                       length: Int)
    extends SparseArray[Double](indices, values, length) {
  def toDense: Array[Double] = super.toDense
}

/** A named sparse array of float values. */
class FloatNamedSparseArray private[java] (indices: Array[Int],
                                           override val values: Array[Float],
                                           length: Int,
                                           names: Seq[String])
    extends NamedSparseArray[Float](indices, values, length, names) {
  def toDense: Array[Float] = super.toDense
}

/** A named sparse array of double values. */
class DoubleNamedSparseArray private[java] (indices: Array[Int],
                                            override val values: Array[Double],
                                            length: Int,
                                            names: Seq[String])
    extends NamedSparseArray[Double](indices, values, length, names) {
  def toDense: Array[Double] = super.toDense
}
