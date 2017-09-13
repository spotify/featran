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

class FeatureCross[T](@transient private val features: Array[Feature[T, _, _, _]])
  extends Serializable {

  private val namedFeatures: Map[String, Int] =
    features.zipWithIndex.map { case (feat, idx) =>
      feat.transformer.name -> idx
    }(scala.collection.breakOut)

  def checkNames(cross: Array[Cross]): Set[String] =
    cross
      .flatMap(c => List(c.name1, c.name2))
      .filterNot(c => namedFeatures.contains(c))
      .toSet

  private val n = features.length
  private val names = features.map(_.transformer.name)

  def values[F, A](
    cross: Array[Cross],
    featureValues: Array[Iterable[(A, Int)]],
    names: Map[Int, Array[String]]
  )(implicit fb: FeatureBuilder[F], fp: FloatingPoint[A]): FeatureBuilder[F] ={
    val iter = cross.iterator
    val index = names.mapValues(_.length)
    fb.init(size(cross, index))

    while(iter.hasNext) {
      val cross = iter.next()
      val fn = cross.combine

      val featureIdx1 = namedFeatures(cross.name1)
      val featureIdx2 = namedFeatures(cross.name2)
      val iter1 = featureValues(featureIdx1).toIterator
      val baseName1 = names(featureIdx1)
      val baseName2 = names(featureIdx2)

      var counter1 = 0
      val length2 = index(featureIdx2)
      while (iter1.hasNext) {
        val (feature1, idx1) = iter1.next()
        if (counter1 < idx1 - 1) fb.skip((idx1 - 1 - counter1) * length2)
        val feature1Double = fp.toDouble(feature1)
        var counter2 = 0
        val iter2 = featureValues(featureIdx2).toIterator
        while (iter2.hasNext) {
          val (feature2, idx2) = iter2.next()
          if (counter2 < idx2 - 1) fb.skip(idx2 - 1 - counter2)
          val feature2Double = fp.toDouble(feature2)
          val name = s"${names(featureIdx1)(idx1)}_${names(featureIdx2)(idx2)}"
          fb.add(name, fn(feature1Double, feature2Double))
          counter2 += 1
        }
        counter1 += 1
      }
    }

    fb
  }

  def names(cross: Array[Cross], names: Map[Int, Array[String]]): Seq[String] = {
    val iter = cross.iterator
    val b = Seq.newBuilder[String]

    while(iter.hasNext){
      val cross = iter.next()

      val featureIdx1 = namedFeatures(cross.name1)
      val featureIdx2 = namedFeatures(cross.name2)
      val baseName1 = names(featureIdx1)
      val baseName2 = names(featureIdx2)

      names(featureIdx1).foreach { name1 =>
        names(featureIdx2).foreach { name2 =>
          val name = s"${name1}_$name2"
          b += name
        }
      }
    }

    b.result()
  }

  def size(cross: Array[Cross], index: Map[Int, Int]): Int = {
    val iter = cross.iterator
    var sum = 0
    while(iter.hasNext){
      val cross = iter.next()

      val idx = namedFeatures(cross.name1)
      val crossIdx = namedFeatures(cross.name2)

      sum += index(idx) * index(crossIdx)
    }

    sum
  }
}
