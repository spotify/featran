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

import scala.collection.mutable

private[transformers] class ThresholdFinder(nLabels: Int,
                                            stoppingCriterion: Double,
                                            maxBins: Int,
                                            minBinWeight: Long)
    extends Serializable {

  class BucketInfo(totals: Seq[Long]) extends Serializable {
    // number of elements in bucket
    lazy val s: Long = totals.sum

    // calculated entropy for the bucket
    lazy val hs: Double = entropy(totals, s)

    // number of distinct classes in bucket
    lazy val k: Long = totals.count(_ != 0)
  }

  private val LOG2 = math.log(2)
  private def log2(x: Double) = math.log(x) / LOG2

  def entropy(frequencies: Seq[Long], n: Long): Double =
    -frequencies.foldLeft(0.0) { (h, q) =>
      if (q == 0) {
        h
      } else {
        val qn = q.toDouble / n
        h + qn * log2(qn)
      }
    }

  def calcCriterionValue(bucketInfo: BucketInfo,
                         leftFreqs: Seq[Long],
                         rightFreqs: Seq[Long]): (Double, Double, Long, Long) = {
    val k1 = leftFreqs.count(_ != 0)
    val s1 = if (k1 > 0) leftFreqs.sum else 0
    val hs1 = entropy(leftFreqs, s1)
    val k2 = rightFreqs.count(_ != 0)
    val s2 = if (k2 > 0) rightFreqs.sum else 0
    val hs2 = entropy(rightFreqs, s2)
    val weightedHs = (s1 * hs1 + s2 * hs2) / bucketInfo.s
    val gain = bucketInfo.hs - weightedHs
    val diff = bucketInfo.k * bucketInfo.hs - k1 * hs1 - k2 * hs2
    val delta = log2(math.pow(3, bucketInfo.k) - 2) - diff
    (gain - (log2(bucketInfo.s - 1) + delta) / bucketInfo.s, weightedHs, s1, s2)
  }

  /**
   * Evaluates boundary points and selects the most relevant candidates (sequential version).
   * Here, the evaluation is bounded by partition as the number of points is small enough.
   *
   * @param candidates candidates points (point, class histogram)
   * @return sequence of threshold values
   */
  def findThresholds(candidates: Seq[(Float, Array[Long])]): Seq[Float] = {
    val queue = new mutable.Queue[((Float, Float), Option[Float])]
    // Insert first in the queue (recursive iteration)
    queue.enqueue(((Float.NegativeInfinity, Float.PositiveInfinity), None))
    var result = List(Float.NegativeInfinity) // # points = # bins - 1

    while (queue.nonEmpty && result.length < maxBins) {
      val (bounds, lastThresh) = queue.dequeue
      // Filter the candidates between the last limits added to the queue
      val newCandidates = candidates.filter {
        case (th, _) =>
          th > bounds._1 && th < bounds._2
      }
      if (newCandidates.length > 0) {
        evalThresholds(newCandidates, lastThresh, nLabels) match {
          case Some(th) =>
            result = th :: result
            queue.enqueue(((bounds._1, th), Some(th)))
            queue.enqueue(((th, bounds._2), Some(th)))
          case None => /* criteria not fulfilled, finish */
        }
      }
    }
    (Float.PositiveInfinity :: result).sorted
  }

  def bestThreshold(entropyFreqs: Seq[(Float, Array[Long], Array[Long], Array[Long])],
                    lastSelected: Option[Float],
                    totals: Array[Long]): Seq[(Double, Float)] = {
    val bucketInfo = new BucketInfo(totals)
    entropyFreqs.flatMap {
      case (cand, _, leftFreqs, rightFreqs) =>
        val duplicate = lastSelected match {
          case None       => false
          case Some(last) => cand == last
        }
        // avoid computing entropy if we have a dupe
        if (duplicate) {
          None
        } else {
          val (criterionValue, weightedHs, leftSum, rightSum) =
            calcCriterionValue(bucketInfo, leftFreqs, rightFreqs)
          val criterion =
            criterionValue > stoppingCriterion && leftSum > minBinWeight && rightSum > minBinWeight
          if (criterion) Some((weightedHs, cand)) else None
        }
    }
  }

  /**
   * Compute entropy minimization for candidate points in a range and select the best one according
   * to MDLP criterion (sequential version).
   * @param candidates array of candidate points (point, class histogram)
   * @param lastSelected last selected threshold
   * @param nLabels number of classes
   * @return the minimum-entropy cut point
   *
   */
  private def evalThresholds(candidates: Seq[(Float, Array[Long])],
                             lastSelected: Option[Float],
                             nLabels: Int): Option[Float] = {
    // Calculate the total frequencies by label
    val totals = Array.fill(nLabels)(0L)
    candidates.foreach(kv => MDLUtil.plusI(totals, kv._2))

    // Compute the accumulated frequencies (both left and right) by label
    var leftAccum = Array.fill(nLabels)(0L)
    var entropyFreqs =
      List.empty[(Float, Array[Long], Array[Long], Array[Long])]
    candidates.foreach {
      case (cand, freq) =>
        leftAccum = MDLUtil.plus(leftAccum, freq)
        val rightTotal = MDLUtil.minus(totals, leftAccum)
        entropyFreqs = (cand, freq, leftAccum, rightTotal) :: entropyFreqs
    }

    // select best threshold according to the criteria
    val finalCandidates = bestThreshold(entropyFreqs, lastSelected, totals)

    // Select among the list of accepted candidate, that with the minimum weightedHs
    if (finalCandidates.nonEmpty) Some(finalCandidates.min._2) else None
  }

}
