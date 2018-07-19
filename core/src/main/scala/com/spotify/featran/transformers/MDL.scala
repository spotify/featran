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

package com.spotify.featran.transformers

import java.util.{TreeMap => JTreeMap}

import com.spotify.featran.{FeatureBuilder, FlatReader, FlatWriter}
import com.spotify.featran.transformers.MinMaxScaler.C
import com.spotify.featran.transformers.mdl.MDLPDiscretizer
import com.spotify.featran.transformers.mdl.MDLPDiscretizer._
import com.twitter.algebird._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

/** Labelled feature for [[MDL]]. */
case class MDLRecord[T](label: T, value: Double)

/**
 * Transform a column of continuous labelled features to n columns of binned categorical features.
 * The optimum number of bins is computed using Minimum Description Length (MDL), which is an
 * entropy measurement between the values and the targets.
 *
 * The transformer expects an [[MDLRecord]] where the first field is a label and the second value
 * is the scalar that will be transformed into buckets.
 *
 * MDL is an iterative algorithm so all of the data needed to compute the buckets will be pulled
 * into memory. If you run into memory issues the `sampleRate` parameter should be lowered.
 *
 * References:
 *
 * - Fayyad, U., & Irani, K. (1993). "Multi-interval discretization of continuous-valued attributes
 * for classification learning."
 *
 * - https://github.com/sramirez/spark-MDLP-discretization
 */
object MDL extends SettingsBuilder {

  /**
   * Create an MDL Instance.
   *
   * @param sampleRate percentage of records to keep to compute the buckets
   * @param seed seed for the sampler
   * @param stoppingCriterion stopping criterion for MDL
   * @param minBinPercentage minimum percent of total data allowed in a single bin
   * @param maxBins maximum number of thresholds per feature
   */
  def apply[T: ClassTag](name: String,
                         sampleRate: Double = 1.0,
                         stoppingCriterion: Double = DefaultStoppingCriterion,
                         minBinPercentage: Double = DefaultMinBinPercentage,
                         maxBins: Int = DefaultMaxBins,
                         seed: Int = Random.nextInt()): Transformer[MDLRecord[T], B[T], C] =
    new MDL(name, sampleRate, stoppingCriterion, minBinPercentage, maxBins, seed)

  /**
   * Create a new [[MDL]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[MDLRecord[String], B[String], C] = {
    val sampleRate = setting.params("sampleRate").toDouble
    val stoppingCriterion = setting.params("stoppingCriterion").toDouble
    val minBinPercentage = setting.params("minBinPercentage").toDouble
    val maxBins = setting.params("maxBins").toInt
    val seed = setting.params("seed").toInt
    MDL[String](setting.name, sampleRate, stoppingCriterion, minBinPercentage, maxBins, seed)
  }

  // Use WrappedArray to workaround Beam immutability enforcement
  private type B[T] = mutable.WrappedArray[MDLRecord[T]]
  private type C = JTreeMap[Double, Int]
}

private[featran] class MDL[T: ClassTag](name: String,
                                        val sampleRate: Double,
                                        val stoppingCriterion: Double,
                                        val minBinPercentage: Double,
                                        val maxBins: Int,
                                        val seed: Int)
    extends Transformer[MDLRecord[T], MDL.B[T], MDL.C](name) {
  checkRange("sampleRate", sampleRate, 0.0, 1.0)
  require(stoppingCriterion >= 0, "stoppingCriterion must be > 0")
  checkRange("minBinPercentage", minBinPercentage, 0.0, 1.0)
  require(maxBins > 0, "maxBins bust be > 0")

  import MDL.{B, C}

  @transient private lazy val rng = {
    val r = new Random(seed)
    r.nextDouble()
    r
  }

  override def featureDimension(c: C): Int = c.size()
  override def featureNames(c: C): Seq[String] = names(c.size())

  def buildFeatures(a: Option[MDLRecord[T]], c: C, fb: FeatureBuilder[_]): Unit = {
    a match {
      case Some(x) =>
        val e = c.higherEntry(x.value)
        val offset = if (e == null) c.size() - 1 else e.getValue
        fb.skip(offset)
        fb.add(nameAt(offset), 1.0)
        fb.skip(c.size() - 1 - offset)
      case None => fb.skip(c.size())
    }
  }

  override val aggregator: Aggregator[MDLRecord[T], B[T], C] =
    new Aggregator[MDLRecord[T], B[T], C] {
      override def prepare(input: MDLRecord[T]): B[T] =
        mutable.WrappedArray
          .make[MDLRecord[T]](if (rng.nextDouble() < sampleRate) Array(input) else Array.empty[T])

      override def semigroup: Semigroup[B[T]] = new Semigroup[B[T]] {
        override def plus(x: B[T], y: B[T]): B[T] = x ++ y
      }

      override def present(reduction: B[T]): C = {
        val ranges = new MDLPDiscretizer[T](
          reduction.view.map(l => (l.label, l.value)),
          stoppingCriterion,
          minBinPercentage
        ).discretize(maxBins)

        val m = new C()
        ranges.tail.zipWithIndex.map { case (v, i) => m.put(v, i) }

        m
      }
    }

  override def encodeAggregator(m: C): String =
    m.asScala.map(kv => s"${kv._1}:${kv._2}").mkString(",")

  override def decodeAggregator(s: String): C = {
    val m = new C()
    s.split(",").foreach { v =>
      val t = v.split(":")
      m.put(t(0).toDouble, t(1).toInt)
    }
    m
  }

  override def params: Map[String, String] =
    Map(
      "sampleRate" -> sampleRate.toString,
      "stoppingCriterion" -> stoppingCriterion.toString,
      "minBinPercentage" -> minBinPercentage.toString,
      "maxBins" -> maxBins.toString,
      "seed" -> seed.toString
    )

  override def flatRead[A: FlatReader]: A => Option[Any] = FlatReader[A].readMdlRecord(name)

  override def flatWriter[A](implicit fw: FlatWriter[A]): Option[MDLRecord[T]] => fw.IF =
    (v: Option[MDLRecord[T]]) =>
      fw.writeMdlRecord(name)(v.map(r => MDLRecord(r.label.toString, r.value)))
}
