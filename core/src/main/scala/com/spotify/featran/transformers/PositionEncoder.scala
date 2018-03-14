package com.spotify.featran.transformers

import com.spotify.featran.{FeatureBuilder, FeatureRejection}

import scala.collection.SortedMap

/**
  * Transform a collection of categorical features to a single value that is the position
  * of that feature within the complete set of categories.
  *
  * Missing values are transformed to zero. Thought may need to be done on how to reject these
  * since the position can overlap the first categorical dimension.
  *
  * When using aggregated feature summary from a previous session, unseen labels are ignored and
  * [[FeatureRejection.Unseen]] rejections are reported.
  */
object PositionEncoder {
  /**
    * Create a new [[PositionEncoder]] instance.
    */
  def apply(name: String): Transformer[String, Set[String], SortedMap[String, Int]] =
    new PositionEncoder(name)
}

private class PositionEncoder(name: String) extends BaseHotEncoder[String](name) {
  override def prepare(a: String): Set[String] = Set(a)
  override def featureDimension(c: SortedMap[String, Int]): Int = 1
  override def featureNames(c: SortedMap[String, Int]): Seq[String] = Seq(name)
  override def buildFeatures(a: Option[String],
                             c: SortedMap[String, Int],
                             fb: FeatureBuilder[_]): Unit = {
    a match {
      case Some(k) => c.get(k) match {
        case Some(v) => fb.add(name, v.toDouble)
        case None =>
          fb.skip(1)
          fb.reject(this, FeatureRejection.Unseen(Set(k)))
      }
      case None =>
        fb.skip(1)
        fb.reject(this, FeatureRejection.Collision)
    }
  }
}
