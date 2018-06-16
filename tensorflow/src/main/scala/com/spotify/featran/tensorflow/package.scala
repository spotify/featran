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

import java.lang.{Long => JLong, Float => JFloat}
import java.nio.{DoubleBuffer, FloatBuffer, LongBuffer}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.tensorflow.example.{Example, Features}
import org.tensorflow.{example => tf}
import org.tensorflow.Tensor

package object tensorflow {

  case class TensorFlowFeatureBuilder(
    @transient private var underlying: Features.Builder = tf.Features.newBuilder())
      extends FeatureBuilder[tf.Example] {
    override def init(dimension: Int): Unit = {
      if (underlying == null) {
        underlying = tf.Features.newBuilder()
      }
      underlying.clear()
    }
    override def add(name: String, value: Double): Unit = {
      val feature = tf.Feature
        .newBuilder()
        .setFloatList(tf.FloatList.newBuilder().addValue(value.toFloat))
        .build()
      underlying.putFeature(name, feature)
    }
    override def skip(): Unit = Unit
    override def skip(n: Int): Unit = Unit
    override def result: tf.Example =
      tf.Example.newBuilder().setFeatures(underlying).build()

    override def newBuilder: FeatureBuilder[Example] = TensorFlowFeatureBuilder()
  }

  case class SparseTensor(indices: Tensor[JLong], values: Tensor[JFloat], shape: Tensor[JLong])

  case class TensorFlowSparseFeatureBuilder() extends FeatureBuilder[SparseTensor] {
    private var indices: Array[Long] = null
    private var values: Array[Float] = null
    private val initCapacity = 1024
    private var dim: Int = _
    private var offset: Int = 0
    private var i: Int = 0

    override def init(dimension: Int): Unit = {
      offset = 0
      i = 0
      //if (indices == null) { why would this ever not be null?
      val n = math.min(dimension, initCapacity)
      indices = new Array[Long](n)
      values = new Array[Float](n)
      //}
    }

    override def add(name: String, value: Double): Unit = {
      indices(i) = offset
      values(i) = FloatingPoint[Float].fromDouble(value)
      i += 1
      offset += 1
      if (indices.length == i) {
        val n = indices.length * 2
        val newIndices = new Array[Long](n)
        val newValues = new Array[Float](n)
        Array.copy(indices, 0, newIndices, 0, indices.length)
        Array.copy(values, 0, newValues, 0, indices.length)
        indices = newIndices
        values = newValues
      }
    }

    override def skip(): Unit = offset += 1

    override def skip(n: Int): Unit = offset += n

    override def result: SparseTensor = {
      val indicesShape = Array[Long](offset)
      val indicesBuffer = LongBuffer.wrap(indices)
      val indicesTensor = Tensor.create(indicesShape, indicesBuffer)

      val valuesShape = Array[Long](offset)
      val valuesBuffer = FloatBuffer.wrap(values)
      val valuesTensor = Tensor.create(valuesShape, valuesBuffer)

      val denseShapeShape = Array[Long](1L)
      val denseShape = LongBuffer.wrap(Array[Long](offset))
      val shapeTensor = Tensor.create(denseShapeShape, denseShape)

      SparseTensor(indicesTensor, valuesTensor, shapeTensor)
    }

    override def newBuilder: FeatureBuilder[SparseTensor] = TensorFlowSparseFeatureBuilder()
  }

  /**
   * [[FeatureBuilder]] for output as TensorFlow `Example` type.
   */
  implicit def tensorFlowFeatureBuilder: FeatureBuilder[tf.Example] = TensorFlowFeatureBuilder()

  implicit def sparseTensorFlowFeatureBuilder[Float]: FeatureBuilder[SparseTensor] =
    TensorFlowSparseFeatureBuilder()

}
