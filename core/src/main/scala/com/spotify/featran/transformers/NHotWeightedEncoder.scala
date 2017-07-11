package com.spotify.featran.transformers

import java.net.{URLDecoder, URLEncoder}

import com.spotify.featran.FeatureBuilder
import com.twitter.algebird.Aggregator

/**
 * Class for a weighted vector.  Also can be thought as
 * a named sparse vector
 * @param name Name of the field
 * @param value Weight on the field
 */
case class WeightedValue(name: String, value: Double)

object NHotWeightedEncoder {
  /**
   * Create a NHotWeightedEncoder with a given name
   *
   * This transformer finds all unique names and maps them to
   * the vector with the weighted value instead of 1.0 as is the
   * case with the normal [[NHotEncoder]].
   *
   * @param name Name of the feature
   * @return Transformer
   */
  def apply(name: String): Transformer[Seq[WeightedValue], Set[String], Array[String]] =
    new NHotWeightedEncoder(name)
}

private class NHotWeightedEncoder(name: String)
  extends Transformer[Seq[WeightedValue], Set[String], Array[String]](name) {

  override val aggregator: Aggregator[Seq[WeightedValue], Set[String], Array[String]] =
    Aggregators.from[Seq[WeightedValue]](_.map(_.name).toSet).to(_.toArray.sorted)

  override def featureDimension(c: Array[String]): Int = c.length

  override def featureNames(c: Array[String]): Seq[String] = c.map(name + "_" + _).toSeq

  override def buildFeatures(a: Option[Seq[WeightedValue]], c: Array[String],
                             fb: FeatureBuilder[_]): Unit = {
    val as: Map[String, Double] = a match {
      case Some(xs) => xs.map(x => (x.name, x.value))(scala.collection.breakOut)
      case None => Map.empty
    }
    c.foreach(s => if (as.contains(s)) fb.add(name + "_" + s, as(s)) else fb.skip())
  }

  override def encodeAggregator(c: Option[Array[String]]): Option[String] =
    c.map(_.map(URLEncoder.encode(_, "UTF-8")).mkString(","))

  override def decodeAggregator(s: Option[String]): Option[Array[String]] =
    s.map(_.split(",").map(URLDecoder.decode(_, "UTF-8")))
}
