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

import breeze.linalg.{DenseVector, SparseVector}
import com.spotify.featran.FeatureBuilderSpec.list
import org.scalacheck._

import scala.reflect.ClassTag

object FeatureBuilderSpec extends Properties("FeatureBuilder") {
  import FeatureBuilder._

  def dense[T](implicit arb: Arbitrary[T]): Gen[List[T]] =
    Gen.listOfN(100, arb.arbitrary)

  def list[T](implicit arb: Arbitrary[Option[T]]): Gen[List[Option[T]]] =
    Gen.listOfN(100, arb.arbitrary)

  def test[T: ClassTag : Numeric, F](xs: List[Option[T]], fb: FeatureBuilder[F])
                                            (toSeq: F => Seq[T]): Prop = {
    val num = implicitly[Numeric[T]]
    fb.init(xs.size + 2)
    xs.zipWithIndex.foreach {
      case (Some(x), i) => fb.add("key" + i.toString, num.toDouble(x))
      case (None, _) => fb.skip()
    }
    fb.skip(2)
    toSeq(fb.result) == xs.map(_.getOrElse(num.zero)) ++ List(num.zero, num.zero)
  }

  def getter[T: ClassTag : Numeric, F](xs: List[T], f: F, fg: FeatureGetter[F, T]): Prop = {
    val equal = xs.zipWithIndex == fg.iterable(Array.empty, f).toList
    val combine = (xs ++ xs).zipWithIndex == fg.iterable(Array.empty, fg.combine(f, f)).toList
    equal && combine
  }

  property("float array") = Prop.forAll(list[Float]) { xs =>
    test(xs, implicitly[FeatureBuilder[Array[Float]]])(_.toSeq)
  }

  property("float array getter") = Prop.forAll(dense[Float]) { xs =>
    val feature = xs.toArray
    getter(xs, feature, implicitly[FeatureGetter[Array[Float], Float]])
  }

  property("double array") = Prop.forAll(list[Double]) { xs =>
    test(xs, implicitly[FeatureBuilder[Array[Double]]])(_.toSeq)
  }

  property("double array getter") = Prop.forAll(dense[Double]) { xs =>
    val feature = xs.toArray
    getter(xs, feature, implicitly[FeatureGetter[Array[Double], Double]])
  }

  property("float traversable") = Prop.forAll(list[Float]) { xs =>
    Prop.all(
      test(xs, implicitly[FeatureBuilder[Traversable[Float]]])(_.toSeq),
      test(xs, implicitly[FeatureBuilder[Iterable[Float]]])(_.toSeq),
      test(xs, implicitly[FeatureBuilder[Seq[Float]]])(identity),
      test(xs, implicitly[FeatureBuilder[IndexedSeq[Float]]])(identity),
      test(xs, implicitly[FeatureBuilder[List[Float]]])(identity),
      test(xs, implicitly[FeatureBuilder[Vector[Float]]])(identity))
  }

  property("double traversable") = Prop.forAll(list[Double]) { xs =>
    Prop.all(
      test(xs, implicitly[FeatureBuilder[Traversable[Double]]])(_.toSeq),
      test(xs, implicitly[FeatureBuilder[Iterable[Double]]])(_.toSeq),
      test(xs, implicitly[FeatureBuilder[Seq[Double]]])(identity),
      test(xs, implicitly[FeatureBuilder[IndexedSeq[Double]]])(identity),
      test(xs, implicitly[FeatureBuilder[List[Double]]])(identity),
      test(xs, implicitly[FeatureBuilder[Vector[Double]]])(identity))
  }

  property("float get traversable") = Prop.forAll(dense[Float]) { xs =>
    getter(xs, xs, implicitly[FeatureGetter[Seq[Float], Float]])
  }

  property("double get traversable") = Prop.forAll(dense[Double]) { xs =>
    getter(xs, xs, implicitly[FeatureGetter[Seq[Double], Double]])
  }

  property("double traversable") = Prop.forAll(list[Double]) { xs =>
    test(xs, implicitly[FeatureBuilder[Seq[Double]]])(identity)
  }

  property("getter float dense vector") = Prop.forAll(dense[Float]) { xs =>
    getter(xs, DenseVector(xs:_*), implicitly[FeatureGetter[DenseVector[Float], Float]])
  }

  property("getter double dense vector") = Prop.forAll(dense[Double]) { xs =>
    getter(xs, DenseVector(xs:_*), implicitly[FeatureGetter[DenseVector[Double], Double]])
  }

  property("float dense vector") = Prop.forAll(list[Float]) { xs =>
    test(xs, implicitly[FeatureBuilder[DenseVector[Float]]])(_.data.toSeq)
  }

  property("double dense vector") = Prop.forAll(list[Double]) { xs =>
    test(xs, implicitly[FeatureBuilder[DenseVector[Double]]])(_.data.toSeq)
  }

  property("getter float sparse vector") = Prop.forAll(dense[Float]) { xs =>
    getter(xs, SparseVector(xs:_*), implicitly[FeatureGetter[SparseVector[Float], Float]])
  }

  property("getter double sparse vector") = Prop.forAll(dense[Double]) { xs =>
    getter(xs, SparseVector(xs:_*), implicitly[FeatureGetter[SparseVector[Double], Double]])
  }

  property("float sparse vector") = Prop.forAll(list[Float]) { xs =>
    test(xs, implicitly[FeatureBuilder[SparseVector[Float]]])(_.toDenseVector.data.toSeq)
  }

  property("double sparse vector") = Prop.forAll(list[Double]) { xs =>
    test(xs, implicitly[FeatureBuilder[SparseVector[Double]]])(_.toDenseVector.data.toSeq)
  }

  property("map") = Prop.forAll(list[Double]) { xs =>
    val fb = implicitly[FeatureBuilder[Map[String, Double]]]
    fb.init(xs.size)
    xs.zipWithIndex.foreach {
      case (Some(x), i) =>
        fb.add("key" + i.toString, x)
      case (None, _) => fb.skip()
    }
    val expected = xs
      .zipWithIndex
      .filter(_._1.isDefined)
      .map(t => ("key" + t._2, t._1.getOrElse(0.0)))
      .toMap
    fb.result == expected
  }

  property("map getter") = Prop.forAll(dense[Double]) { xs =>
    val indexed = xs.zipWithIndex
    val names = indexed.map(_._2.toString).toArray
    val map = indexed.map{case(v, idx) => idx.toString -> v}.toMap
    val getter = implicitly[FeatureGetter[Map[String, Double], Double]]

    val equal = indexed == getter.iterable(names, map)
    val combine = indexed == getter.iterable(names, getter.combine(map, map)).toList
    equal && combine
  }

}

