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

package com.spotify.featran.transformers.mdl

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

private[transformers] class MDLPDiscretizer[T: ClassTag](
  data: Seq[(T, Double)],
  stoppingCriterion: Double = MDLPDiscretizer.DefaultStoppingCriterion,
  minBinPercentage: Double = MDLPDiscretizer.DefaultMinBinPercentage
) extends Serializable {

  private val labels = {
    val m = mutable.Map.empty[T, Int]
    data.foreach {
      case (k, _) =>
        if (!m.contains(k)) {
          m(k) = m.size
        }
    }
    m
  }

  private def isBoundary(f1: Array[Long], f2: Array[Long]): Boolean = {
    val l = math.min(f1.length, f2.length)
    var count = 0
    var i = 0
    while (i < l && count <= 1) {
      if (f1(i) + f2(i) != 0) {
        count += 1
      }
      i += 1
    }
    count > 1
  }

  private def midpoint(x1: Float, x2: Float): Float = (x1 + x2) / 2.0f

  def discretize(maxBins: Int = MDLPDiscretizer.DefaultMaxBins): Seq[Double] = {
    val featureValues = new java.util.TreeMap[Float, Array[Long]]()
    data.foreach {
      case (label, value) =>
        val key = value.toFloat
        val i = labels(label)
        val x = featureValues.get(key)
        if (x == null) {
          val y = Array.fill(labels.size)(0L)
          y(i) = 1L
          featureValues.put(key, y)
        } else {
          x(i) += 1L
        }
    }

    val cutPoint = if (!featureValues.isEmpty) {
      val it = featureValues.asScala.iterator
      var (lastX, lastFreqs) = it.next()
      var result = List.empty[(Float, Array[Long])]
      var accumFreqs = lastFreqs
      while (it.hasNext) {
        val (x, freqs) = it.next()
        if (isBoundary(freqs, lastFreqs)) {
          result = (midpoint(x, lastX), accumFreqs) :: result
          accumFreqs = Array.fill(labels.size)(0L)
        }
        lastX = x
        lastFreqs = freqs
        MDLUtil.plusI(accumFreqs, freqs)
      }
      (lastX, accumFreqs) :: result
    } else {
      Nil
    }

    val minBinWeight: Long = (minBinPercentage * data.length / 100.0).toLong
    val finder =
      new ThresholdFinder(labels.size, stoppingCriterion, maxBins, minBinWeight)
    finder.findThresholds(cutPoint.sortBy(_._1)).map(_.toDouble)
  }

}

private[transformers] object MDLPDiscretizer {
  val DefaultStoppingCriterion: Double = 0.0
  val DefaultMinBinPercentage: Double = 0.0
  val DefaultMaxBins: Int = 50
}
