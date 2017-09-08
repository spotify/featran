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

class FeatureCross[T](
  @transient private val features: Array[Feature[T, _, _, _]]) extends Serializable {

  val namedFeatures: Map[String, Int] =
    features.zipWithIndex.map { case (feat, idx) =>
      feat.transformer.name -> idx
    }(scala.collection.breakOut)

  def crossValues[F: FeatureGetter: FeatureBuilder](
    cross: Array[Cross],
    names: Array[String],
    fg: F,
    index: Map[Int, Range]): F = {
    val fb = implicitly[FeatureBuilder[F]]
    val getter = implicitly[FeatureGetter[F]]

    val iter = cross.iterator
    fb.init(crossSize(cross, index))
    while(iter.hasNext){
      values(iter.next(), names, fg, fb, index)
    }
    val crossed = fb.result
    getter.combine(fg, crossed)
  }

  def values[F: FeatureGetter](
    cross: Cross,
    names: Array[String],
    fg: F,
    fb: FeatureBuilder[F],
    index: Map[Int, Range]): Unit = {

    val getter = implicitly[FeatureGetter[F]]
    val featureIdx1 = namedFeatures(cross.name1)
    val featureIdx2 = namedFeatures(cross.name2)

    val range1 = index(featureIdx1)
    val range2 = index(featureIdx2)

    val fn = cross.combine

    var idx1 = range1.start
    var idx2 = range2.start

    range1.foreach{idx1 =>
      val name1 = names(featureIdx1)
      val v1 = getter.get(name1, idx1, fg)
      range2.foreach{idx2 =>
        val name2 = names(featureIdx2)
        val v2 = getter.get(name2, idx1, fg)
        fb.add(name1 + "-" + name2, fn(v1, v2))
      }
    }

  }

  def crossNames(
    cross: Array[Cross],
    currentNames: Array[String],
    index: Map[Int, Range]): Seq[String] = {

    val iter = cross.iterator
    val b = Seq.newBuilder[String]
    while(iter.hasNext){
      b ++= names(iter.next(), currentNames, index)
    }
    currentNames ++ b.result()
  }

  def names(
    cross: Cross,
    names: Array[String],
    index: Map[Int, Range]): Seq[String] = {

    val featureIdx1 = namedFeatures(cross.name1)
    val featureIdx2 = namedFeatures(cross.name2)

    val range1 = index(featureIdx1)
    val range2 = index(featureIdx2)

    var idx1 = range1.start
    var idx2 = range2.start

    val b = Seq.newBuilder[String]

    range1.foreach{idx1 =>
      val name1 = names(featureIdx1)
      range2.foreach{idx2 =>
        val name2 = names(featureIdx2)
        b += name1 + "-" + name2
      }
    }

    b.result()
  }

  def crossSize(cross: Array[Cross], index: Map[Int, Range]): Int = {
    val iter = cross.iterator
    var sum = 0
    while(iter.hasNext){
      sum += size(iter.next(), index)
    }
    sum
  }

  def size(cross: Cross, index: Map[Int, Range]): Int = {
    val idx = namedFeatures(cross.name1)
    val crossIdx = namedFeatures(cross.name2)
    val rightDim = index(idx)
    val leftDim = index(crossIdx)

    rightDim.length * leftDim.length
  }
}
