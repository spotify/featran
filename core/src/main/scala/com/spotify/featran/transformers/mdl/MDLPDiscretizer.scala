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

import MDLPDiscretizer._
import scala.reflect.ClassTag

class MDLPDiscretizer[T: ClassTag](
    val data: List[(T, Double)],
    stoppingCriterion: Double = DEFAULT_STOPPING_CRITERION,
    minBinPercentage: Double = DEFAULT_MIN_BIN_PERCENTAGE
  ) extends Serializable {

  private val labels =
    data
      .map(_._1)
      .distinct
      .zipWithIndex
      .toMap

  private val labelCount = labels.size
  private val counts = data.length

  private val isBoundary = (f1: Array[Long], f2: Array[Long]) =>
    (f1, f2).zipped.map(_ + _).count(_ != 0) > 1

  private def midpoint(x1: Float, x2: Float): Float = {
    if (x1.isNaN){
      x2
    } else if (x2.isNaN) {
      x1
    }
    else {
      (x1 + x2) / 2.0F
    }
  }

  def discretize(maxBins: Int = MAX_BINS): List[Double] = {
    val featureValues = data.map{case(label, dv) =>
      val c = Array.fill[Long](labels.size)(0L)
      c(labels(label)) = 1L
      (dv.toFloat, c)
    }

    val v = featureValues
      .groupBy(_._1)
      .mapValues(v => v.map(_._2).reduce{(a, b) => (a, b).zipped.map(_ + _)})

    val sorted = v.toList.sortBy(_._1)
    val it = sorted.toIterator

    val cutpoint = if (it.hasNext) {
      var (lastX, lastFreqs) = it.next()
      var result = Seq.empty[(Float, Array[Long])]
      var accumFreqs = lastFreqs
      while(it.hasNext){
        val (x, freqs) = it.next()
        if (isBoundary(freqs, lastFreqs)) {
          result = (midpoint(x, lastX), accumFreqs.clone) +: result
          accumFreqs = Array.fill(labelCount)(0L)
        }
        lastX = x
        lastFreqs = freqs
        accumFreqs = (accumFreqs, freqs).zipped.map(_ + _)
      }
      result = (lastX, accumFreqs.clone) +: result
      result.reverse.toList
    } else {
      Nil
    }

    val minBinWeight: Long = (minBinPercentage * counts.toInt / 100.0).toLong
    val finder = new ThresholdFinder(labelCount, stoppingCriterion, maxBins, minBinWeight)
    finder
      .findThresholds(cutpoint.toArray.sortBy(_._1))
      .map(_.toDouble)
      .toList
  }
}

object MDLPDiscretizer {
  val MAX_BINS = 50
  val DEFAULT_STOPPING_CRITERION = 0
  val DEFAULT_MIN_BIN_PERCENTAGE = 0
}

