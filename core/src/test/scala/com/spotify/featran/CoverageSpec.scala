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

import com.spotify.featran.transformers._
import org.scalacheck._

import scala.util.Try

// Hack to maintain 100% coverage
object CoverageSpec extends Properties("Coverage") {

  {
    // RecordExtractor.iteratorCollectionType#reduce
    val fs = FeatureSpec.of[Double].required(identity)(Identity("id"))
    val settings = fs.extract(Seq(1.0)).featureSettings.head
    val e = fs.extractWithSettings[Seq[Double]](settings)
    val f =
      e.getClass.getDeclaredField("com$spotify$featran$RecordExtractor$$iteratorCollectionType")
    f.setAccessible(true)
    val ct = f.get(e).asInstanceOf[CollectionType[Iterator]]
    require(Try(ct.reduce[Double](Iterator(1.0, 1.0))(_ + _)).isFailure)
  }

  {
    // RecordExtractor.iteratorCollectionType#pure
    val fs = FeatureSpec.of[Double].required(identity)(Identity("id"))
    val settings = fs.extract(Seq(1.0)).featureSettings.head
    val e = fs.extractWithSettings[Seq[Double]](settings)
    val f =
      e.getClass.getDeclaredField("com$spotify$featran$RecordExtractor$$iteratorCollectionType")
    f.setAccessible(true)
    val ct = f.get(e).asInstanceOf[CollectionType[Iterator]]
    require(ct.pure(Iterator())(1.0).nonEmpty)
  }

  {
    // QuantileOutlierRejector when both rejectLower and rejectUpper are false
    val t = QuantileOutlierRejector("q")
    val f1 = t.getClass.getDeclaredField("rejectLower")
    f1.setAccessible(true)
    f1.setBoolean(t, false)
    val f2 = t.getClass.getDeclaredField("rejectUpper")
    f2.setAccessible(true)
    f2.setBoolean(t, false)
    val fb = FeatureBuilder[Array[Double]]
    fb.init(1)
    t.buildFeatures(Some(0), (0, 0, Double.MinValue, Double.MaxValue), fb)
  }

  {
    val fb = FeatureBuilder[Seq[Double]]
    val f = classOf[CrossingFeatureBuilder[_]]
      .getConstructor(classOf[FeatureBuilder[_]], classOf[Crossings])
    f.setAccessible(true)
    val cfb = f.newInstance(fb, Crossings.empty)
    cfb.newBuilder
  }

}
