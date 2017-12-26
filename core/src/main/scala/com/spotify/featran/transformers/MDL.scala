package com.spotify.featran.transformers

import com.spotify.featran.FeatureBuilder
import com.spotify.featran.transformers.mdl.MDLPDiscretizer
import com.spotify.featran.transformers.mdl.MDLPDiscretizer._
import com.twitter.algebird._
import spire.ClassTag

import scala.collection.JavaConverters._
import scala.util.Random

case class MDLRecord[T](label: T, value: Double)
import java.util.{TreeMap => JTreeMap}

/**
  * This Transformer computes the optimal number of buckets using
  * minimum description length. That is an entropy measurement between
  * the values and the tagets.
  *
  * The Transformer expects an MDLRecord where the first field is a label
  * and the second value is the scalar that will be transformed into buckets.
  *
  * MDL is an iterative algorithm so all of the data needed to compute the buckets
  * will need to be pulled into memory.  If you run into memory issues the sampleRate
  * parameter will need to be lowered.
  */
object MDL {
  /**
    * Create an MDL Instance
    * @param name Name of the Transformer
    * @param sampleRate The percentage of records to keep to compute the buckets
    * @param seed Seed for the sampler
    * @param stoppingCriterion Stopping criterion for MDLP
    * @param minBinPercentage Min Number of bins
    * @param maxBins Max number of bins
    * @tparam T Generic type for the label
    */
  def apply[T: ClassTag](
    name: String,
    sampleRate: Double = 1.0,
    seed: Int = 1,
    stoppingCriterion: Double = DEFAULT_STOPPING_CRITERION,
    minBinPercentage: Double = DEFAULT_MIN_BIN_PERCENTAGE,
    maxBins: Int = MAX_BINS)
  : Transformer[MDLRecord[T], List[MDLRecord[T]], JTreeMap[Double, Int]] =
    new MDL(name, sampleRate, seed, stoppingCriterion, minBinPercentage, maxBins)
}

private class MDL[T: ClassTag](
  name: String,
  val sampleRate: Double,
  val seed: Int,
  stoppingCriterion: Double = DEFAULT_STOPPING_CRITERION,
  minBinPercentage: Double = DEFAULT_MIN_BIN_PERCENTAGE,
  maxBins: Int = MAX_BINS)
  extends Transformer[MDLRecord[T], List[MDLRecord[T]], JTreeMap[Double, Int]](name) {

  private lazy val random = {
    val r = new Random(seed)
    r.nextDouble()
    r
  }

  override def featureDimension(c: JTreeMap[Double, Int]): Int = c.size()

  override def featureNames(c: JTreeMap[Double, Int]): Seq[String] = names(c.size())

  def buildFeatures(a: Option[MDLRecord[T]], c: JTreeMap[Double, Int], fb: FeatureBuilder[_]) {
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

  val aggregator = new Aggregator[MDLRecord[T], List[MDLRecord[T]], JTreeMap[Double, Int]] {
    override def prepare(input: MDLRecord[T]): List[MDLRecord[T]] =
      if(random.nextDouble() < sampleRate) List(input) else Nil

    override def semigroup: Semigroup[List[MDLRecord[T]]] = new Semigroup[List[MDLRecord[T]]] {
      override def plus(x: List[MDLRecord[T]], y: List[MDLRecord[T]]): List[MDLRecord[T]] = {
        x ++ y
      }
    }

    override def present(reduction: List[MDLRecord[T]]): JTreeMap[Double, Int] = {
      val ranges = new MDLPDiscretizer[T](
        reduction.map(l => (l.label, l.value)),
        stoppingCriterion,
        minBinPercentage
      ).discretize(maxBins)

      val m = new JTreeMap[Double, Int]()
      ranges
        .tail
        .zipWithIndex
        .map{case(v, i) => m.put(v, i)}

      m
    }
  }

  override def encodeAggregator(m: JTreeMap[Double, Int]): String =
    m.asScala.map(kv => s"${kv._1}:${kv._2}").mkString(",")

  override def decodeAggregator(s: String): JTreeMap[Double, Int] = {
    val m = new JTreeMap[Double, Int]()
    s.split(",").foreach { v =>
      val t = v.split(":")
      m.put(t(0).toDouble, t(1).toInt)
    }
    m
  }

  override def params: Map[String, String] = Map(
    "seed" -> seed.toString,
    "sampleRate" -> sampleRate.toString,
    "stoppingCriterion" -> stoppingCriterion.toString,
    "minBinPercentage" -> minBinPercentage.toString,
    "maxBins" -> maxBins.toString)
}

