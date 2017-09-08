package com.spotify.featran

import scala.reflect.ClassTag
import scala.language.{higherKinds, implicitConversions}

case class CrossFeatureSpec[T](spec: FeatureSpec[T], cross: List[Cross]) {
  def extract[M[_]: CollectionType](input: M[T]): CrossedFeatureExtractor[M, T] =
    new CrossedFeatureExtractor[M, T](new FeatureSet[T](spec.features), input, None, cross)

  def extractWithSettings[M[_]: CollectionType](input: M[T], settings: M[String])
  : CrossedFeatureExtractor[M, T] = {
    val set = new FeatureSet(spec.features)
    new CrossedFeatureExtractor[M, T](set, input, Some(settings), cross)
  }
}

class CrossedFeatureExtractor[M[_]: CollectionType, T] private[featran]
(private val fs: FeatureSet[T],
 @transient private val input: M[T],
 @transient private val settings: Option[M[String]],
 @transient private val cross: List[Cross]
) extends Serializable {

  private val extractor = new FeatureExtractor(fs, input, settings)

  @transient private val dt: CollectionType[M] = implicitly[CollectionType[M]]
  import dt.Ops._

  def featureValues[F: FeatureBuilder : ClassTag : FeatureGetter]: M[F] =
    featureResults.map(_.value)

  def featureResults[F: FeatureBuilder : ClassTag : FeatureGetter]: M[FeatureResult[F, T]] = {
    val fb = implicitly[FeatureBuilder[F]]
    val cls = fs
    extractor
      .featureResults
      .cross(extractor.aggregate)
      .cross(featureNames)
      .map{case((FeatureResult(value, rej, orig), c), names) =>
        val index = fs.featureDimensionIndex(c).toMap
        val values = FeatureCross(cls, cross, names.toArray).crossValues(value, index)
        FeatureResult(values, rej, orig)
      }
  }

  @transient lazy val featureNames: M[Seq[String]] = {
    val cls = fs
    extractor.aggregate.map { a =>
      val names = fs.featureNames(a)
      val index = fs.featureDimensionIndex(a).toMap
      FeatureCross(cls, cross, names.toArray).crossNames(index)
    }
  }

  @transient lazy val featureSettings: M[String] = extractor.featureSettings
}
