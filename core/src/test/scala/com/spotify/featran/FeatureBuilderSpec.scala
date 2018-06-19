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
import org.scalacheck._

import scala.reflect.ClassTag

object FeatureBuilderSpec extends Properties("FeatureBuilder") {

  private def list[T](implicit arb: Arbitrary[Option[T]]): Gen[List[Option[T]]] =
    Gen.listOfN(100, arb.arbitrary)

  private def test[T: ClassTag: Numeric, F](xs: List[Option[T]], builder: FeatureBuilder[F])(
    toSeq: F => Seq[T]): Prop = {
    val fb = SerializableUtils.ensureSerializable(builder)
    val num = implicitly[Numeric[T]]
    fb.init(xs.size + 4)
    fb.prepare(null)
    xs.zipWithIndex.foreach {
      case (Some(x), i) => fb.add("key" + i.toString, num.toDouble(x))
      case (None, _)    => fb.skip()
    }
    fb.add(Iterable("x", "y"), Seq(0.0, 0.0))
    fb.skip(2)
    toSeq(fb.result) == xs.map(_.getOrElse(num.zero)) ++ List.fill(4)(num.zero)
  }

  property("float array") = Prop.forAll(list[Float]) { xs =>
    test(xs, FeatureBuilder[Array[Float]])(_.toSeq)
  }

  property("double array") = Prop.forAll(list[Double]) { xs =>
    test(xs, FeatureBuilder[Array[Double]])(_.toSeq)
  }

  property("float traversable") = Prop.forAll(list[Float]) { xs =>
    Prop.all(
      test(xs, FeatureBuilder[Traversable[Float]])(_.toSeq),
      test(xs, FeatureBuilder[Iterable[Float]])(_.toSeq),
      test(xs, FeatureBuilder[Seq[Float]])(identity),
      test(xs, FeatureBuilder[IndexedSeq[Float]])(identity),
      test(xs, FeatureBuilder[List[Float]])(identity),
      test(xs, FeatureBuilder[Vector[Float]])(identity)
    )
  }

  property("double traversable") = Prop.forAll(list[Double]) { xs =>
    Prop.all(
      test(xs, FeatureBuilder[Traversable[Double]])(_.toSeq),
      test(xs, FeatureBuilder[Iterable[Double]])(_.toSeq),
      test(xs, FeatureBuilder[Seq[Double]])(identity),
      test(xs, FeatureBuilder[IndexedSeq[Double]])(identity),
      test(xs, FeatureBuilder[List[Double]])(identity),
      test(xs, FeatureBuilder[Vector[Double]])(identity)
    )
  }

  property("double traversable") = Prop.forAll(list[Double]) { xs =>
    test(xs, FeatureBuilder[Seq[Double]])(identity)
  }

  property("float sparse array") = Prop.forAll(list[Float]) { xs =>
    test(xs, FeatureBuilder[SparseArray[Float]])(_.toDense.toSeq)
    val n = 1024 / xs.size + 1
    val xs2 = Seq.fill(n)(xs).reduce(_ ++ _)
    test(xs2, FeatureBuilder[SparseArray[Float]])(_.toDense.toSeq)
  }

  property("double sparse array") = Prop.forAll(list[Double]) { xs =>
    test(xs, FeatureBuilder[SparseArray[Double]])(_.toDense.toSeq)
    val n = 1024 / xs.size + 1
    val xs2 = Seq.fill(n)(xs).reduce(_ ++ _)
    test(xs2, FeatureBuilder[SparseArray[Double]])(_.toDense.toSeq)
  }

  property("float named sparse array") = Prop.forAll(list[Float]) { xs =>
    test(xs, FeatureBuilder[NamedSparseArray[Float]])(_.toDense.toSeq)
    val n = 1024 / xs.size + 1
    val xs2 = Seq.fill(n)(xs).reduce(_ ++ _)
    test(xs2, FeatureBuilder[NamedSparseArray[Float]])(_.toDense.toSeq)
  }

  property("double named sparse array") = Prop.forAll(list[Double]) { xs =>
    test(xs, FeatureBuilder[NamedSparseArray[Double]])(_.toDense.toSeq)
    val n = 1024 / xs.size + 1
    val xs2 = Seq.fill(n)(xs).reduce(_ ++ _)
    test(xs2, FeatureBuilder[NamedSparseArray[Double]])(_.toDense.toSeq)
  }

  property("named sparse array") = Prop.forAll(list[Double]) { xs =>
    val fb = FeatureBuilder[NamedSparseArray[Double]]
    fb.init(xs.size + 4)
    xs.zipWithIndex.foreach {
      case (Some(x), i) =>
        fb.add("key" + i.toString, x)
      case (None, _) => fb.skip()
    }
    fb.add(Iterable("x", "y"), Seq(0.0, 0.0))
    fb.skip(2)
    val actual = fb.result
    val nonZeros = xs.zipWithIndex.filter(_._1.isDefined)
    val indices = nonZeros.map(_._2) ++ Seq(xs.size, xs.size + 1)
    val values = nonZeros.map(_._1.get) ++ Seq(0.0, 0.0)
    val names = nonZeros.map(t => "key" + t._2) ++ Seq("x", "y")
    Prop.all(actual.indices.toSeq == indices,
             actual.values.toSeq == values,
             actual.length == xs.size + 4,
             actual.names == names)
  }

  property("float dense vector") = Prop.forAll(list[Float]) { xs =>
    test(xs, FeatureBuilder[DenseVector[Float]])(_.data.toSeq)
  }

  property("double dense vector") = Prop.forAll(list[Double]) { xs =>
    test(xs, FeatureBuilder[DenseVector[Double]])(_.data.toSeq)
  }

  property("float sparse vector") = Prop.forAll(list[Float]) { xs =>
    test(xs, FeatureBuilder[SparseVector[Float]])(_.toDenseVector.data.toSeq)
  }

  property("double sparse vector") = Prop.forAll(list[Double]) { xs =>
    test(xs, FeatureBuilder[SparseVector[Double]])(_.toDenseVector.data.toSeq)
  }

  property("map") = Prop.forAll(list[Double]) { xs =>
    val fb = FeatureBuilder[Map[String, Double]]
    fb.init(xs.size + 4)
    xs.zipWithIndex.foreach {
      case (Some(x), i) =>
        fb.add("key" + i.toString, x)
      case (None, _) => fb.skip()
    }
    fb.add(Iterable("x", "y"), Seq(0.0, 0.0))
    fb.skip(2)
    val actual = fb.result
    val expected = xs.zipWithIndex
      .filter(_._1.isDefined)
      .map(t => ("key" + t._2, t._1.getOrElse(0.0)))
      .toMap ++ Map("x" -> 0.0, "y" -> 0.0)
    Prop.all(actual == expected,
             actual + ("z" -> 1.0) == expected + ("z" -> 1.0),
             actual - "x" == expected - "x",
             expected.forall(kv => actual(kv._1) == kv._2))
  }

}
