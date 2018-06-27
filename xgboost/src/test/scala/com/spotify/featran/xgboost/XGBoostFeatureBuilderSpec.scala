/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.featran.xgboost

import com.spotify.featran.{FeatureBuilder, SerializableUtils, SparseArray}
import ml.dmlc.xgboost4j.LabeledPoint
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}

import scala.reflect.ClassTag

object XGBoostFeatureBuilderSpec extends Properties("XGBoostFeatureBuilder") {

  private def list[T](implicit arb: Arbitrary[Option[T]]): Gen[List[Option[T]]] =
    Gen.listOfN(100, arb.arbitrary)

  private def test[T: ClassTag: Numeric, F](xs: List[Option[T]], builder: FeatureBuilder[F])(
    toSeq: F => Seq[Float]): Prop = {
    val num = implicitly[Numeric[T]]
    val fb = SerializableUtils.ensureSerializable(builder)
    fb.init(xs.size + 4)
    fb.prepare(null)
    xs.zipWithIndex.foreach {
      case (Some(x), i) => fb.add("key" + i.toString, num.toDouble(x))
      case (None, _)    => fb.skip()
    }
    fb.add(Iterable("x", "y"), Seq(0.0, 0.0))
    fb.skip(2)
    // keep in mind that we force the RHS to be floats because that is what LabeledPoint stores
    toSeq(fb.result) == (xs.map(_.getOrElse(num.zero)) ++ List.fill(4)(num.zero)).map(num.toFloat)
  }

  property("LabeledPoint on Float input") = Prop.forAll(list[Float]) { xs =>
    test(xs, FeatureBuilder[LabeledPoint])(_.values.toSeq)
  }

  property("LabeledPoint on Double input") = Prop.forAll(list[Double]) { xs =>
    test(xs, FeatureBuilder[LabeledPoint])(_.values.toSeq)
  }

  property("Sparse LabeledPoint on Float input") = Prop.forAll(list[Float]) { xs =>
    test(xs, FeatureBuilder[SparseLabeledPoint])(r =>
      SparseArray(r.labeledPoint.indices, r.labeledPoint.values, 4 + xs.size).toDense.toSeq)
    val n = 1024 / xs.size + 1
    val xs2 = Seq.fill(n)(xs).reduce(_ ++ _)
    test(xs2, FeatureBuilder[SparseLabeledPoint])(r =>
      SparseArray(r.labeledPoint.indices, r.labeledPoint.values, 4 + xs2.size).toDense.toSeq)
  }

  property("Sparse LabeledPoint on Double input") = Prop.forAll(list[Double]) { xs =>
    test(xs, FeatureBuilder[SparseLabeledPoint])(r =>
      SparseArray(r.labeledPoint.indices, r.labeledPoint.values, 4 + xs.size).toDense.toSeq)
    val n = 1024 / xs.size + 1
    val xs2 = Seq.fill(n)(xs).reduce(_ ++ _)
    test(xs2, FeatureBuilder[SparseLabeledPoint])(r =>
      SparseArray(r.labeledPoint.indices, r.labeledPoint.values, 4 + xs2.size).toDense.toSeq)
  }

}
