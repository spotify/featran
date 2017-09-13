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

import com.spotify.featran.transformers.Settings

import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag


/**
 * Encapsulate features extracted from a [[FeatureSpec]].
 * @tparam M input collection type, e.g. `Array`, List
 * @tparam T input record type to extract features from
 */
class FeatureExtractor[M[_]: CollectionType, T] private[featran]
(private val fs: FeatureSet[T],
 @transient private val input: M[T],
 @transient private val settings: Option[M[String]],
 private val crosses: Array[Cross])
  extends Serializable {

  import FeatureSpec.ARRAY

  private val featureCross = new FeatureCross(fs.features)

  {
    val missingNames = featureCross.checkNames(crosses)
    require(
      missingNames.isEmpty,
      "Crossed Features Names must exist as features, Missing : " + missingNames.mkString(",")
    )
  }

  @transient private val dt: CollectionType[M] = implicitly[CollectionType[M]]
  import dt.Ops._

  @transient private[featran] lazy val as: M[(T, ARRAY)] = {
    val g = fs // defeat closure
    input.map{o => (o, g.unsafeGet(o))}
  }

  @transient private[featran] lazy val aggregate: M[ARRAY] = settings match {
    case Some(x) =>
      val o = fs
      x.map { s =>
        import io.circe.generic.auto._
        import io.circe.parser._
        o.decodeAggregators(decode[Seq[Settings]](s).right.get)
      }
    case None =>
      val o = fs
      as
        .map(t => o.unsafePrepare(t._2))
        .reduce(o.unsafeSum)
        .map(o.unsafePresent)
  }

  /**
   * JSON settings of the [[FeatureSpec]] and aggregated feature summary.
   *
   * This can be used with [[FeatureSpec.extractWithSettings]] to bypass the `reduce` step when
   * extracting new records of the same type.
   */
  @transient lazy val featureSettings: M[String] = settings match {
    case Some(x) => x
    case None => aggregate.map { a =>
      val o = fs
      import io.circe.generic.auto._
      import io.circe.syntax._
      o.featureSettings(a).asJson.noSpaces
    }
  }

  /**
   * Names of the extracted features, in the same order as values in [[featureValues]].
   */
  @transient lazy val featureNames: M[Seq[String]] = {
    val o = fs
    val crossArray = crosses
    aggregate.map { aggr =>
      val names = o.featureIndexedNames(aggr)
      o.featureNames(aggr) ++ featureCross.names(crossArray, names)
    }
  }

  /**
   * Values of the extracted features, in the same order as names in [[featureNames]].
   * @tparam F output data type, e.g. `Array[Float]`, `Array[Double]`, `DenseVector[Float]`,
   *           `DenseVector[Double]`
   */
  def featureValues[F, A]
  (implicit fb: FeatureBuilder[F], fg: FeatureGetter[F, A], ct: ClassTag[F], fp: FloatingPoint[A])
  : M[F] = featureResults.map(_.value)

  /**
   * Values of the extracted features, in the same order as names in [[featureNames]] with
   * rejections keyed on feature name and the original input record.
   * @tparam F output data type, e.g. `Array[Float]`, `Array[Double]`, `DenseVector[Float]`,
   *           `DenseVector[Double]`
   */
  def featureResults[F, A]
  (implicit fb: FeatureBuilder[F], fg: FeatureGetter[F, A], ct: ClassTag[F], fp: FloatingPoint[A])
  : M[FeatureResult[F, T]] = {
    val cls = fs
    val n = fs.features.length
    val crossArray = crosses
    val namedAggregate = aggregate.map{aggr => (aggr, cls.featureIndexedNames(aggr))}

    as.cross(namedAggregate).map { case ((o, a), (c, names)) =>
      val fbs = FeatureBuilder[F](n)

      cls.featureValues(a, c, fbs)

      if(crossArray.nonEmpty){
        val results = fbs.map(v => (v.result, v.rejections))
        val iterables = results.zipWithIndex.map{case((f, _), idx) => fg.iterable(names(idx), f)}
        val crossResult = featureCross.values(crossArray, iterables, names)
        val (reduced, rejections) = results.reduceLeft[(F, Map[String, FeatureRejection])]
          {case((cres, crej), (res, rej)) => (fg.combine(cres, res), crej ++ rej)}

        FeatureResult(
          fg.combine(reduced, crossResult.result),
          rejections ++ crossResult.rejections,
          o)
      } else {
        val (results, rejections) = fbs
          .map(v => (v.result, v.rejections))
          .reduceLeft[(F, Map[String, FeatureRejection])]
            {case((cres, crej), (res, rej)) => (fg.combine(cres, res), crej ++ rej)}

        FeatureResult(results, rejections, o)
      }
    }
  }

}

case class FeatureResult[F, T](value: F, rejections: Map[String, FeatureRejection], original: T)