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

import com.spotify.featran.transformers.{NHotEncoder, VectorIdentity}
import org.scalacheck._

import scala.collection.SortedSet

object CrossingSpec extends Properties("CrossingSpec") {

  implicit def arbList[T: Arbitrary]: Arbitrary[List[T]] =
    Arbitrary(Gen.listOfN(10, Arbitrary.arbitrary[T]))

  implicit val arbDouble: Arbitrary[Double] = Arbitrary(Gen.choose(-1.0, 1.0))

  type A = ((Double, Double), (Double, Double, Double))
  private def flatten(v: (Double, Double)) = Array(v._1, v._2)
  private def flatten(v: (Double, Double, Double)) = Array(v._1, v._2, v._3)

  private def denseNames(a: String, b: String) =
    Seq(
      s"${a}_0",
      s"${a}_1",
      s"${b}_0",
      s"${b}_1",
      s"${b}_2",
      s"cross_${a}_0_x_${b}_0",
      s"cross_${a}_0_x_${b}_1",
      s"cross_${a}_0_x_${b}_2",
      s"cross_${a}_1_x_${b}_0",
      s"cross_${a}_1_x_${b}_1",
      s"cross_${a}_1_x_${b}_2"
    )
  private def denseValues(v: A, f: (Double, Double) => Double) =
    Seq(
      v._1._1,
      v._1._2,
      v._2._1,
      v._2._2,
      v._2._3,
      f(v._1._1, v._2._1),
      f(v._1._1, v._2._2),
      f(v._1._1, v._2._3),
      f(v._1._2, v._2._1),
      f(v._1._2, v._2._2),
      f(v._1._2, v._2._3)
    )

  property("dense") = Prop.forAll { xs: List[A] =>
    val f1 = FeatureSpec
      .of[A]
      .required(t => flatten(t._1))(VectorIdentity("a"))
      .required(t => flatten(t._2))(VectorIdentity("b"))
      .cross(("a", "b"))(_ * _)
      .extract(xs)
    val f2 = FeatureSpec
      .of[A]
      .required(t => flatten(t._1))(VectorIdentity("a"))
      .required(t => flatten(t._2))(VectorIdentity("b"))
      .cross(("a", "b"))(_ + _)
      .extract(xs)
    Prop.all(
      f1.featureNames == Seq(denseNames("a", "b")),
      f1.featureValues[Seq[Double]] == xs.map(denseValues(_, _ * _)),
      f2.featureNames == Seq(denseNames("a", "b")),
      f2.featureValues[Seq[Double]] == xs.map(denseValues(_, _ + _))
    )
  }

  property("dense multi") = Prop.forAll { xs: List[A] =>
    val f1 = FeatureSpec
      .of[A]
      .required(t => flatten(t._1))(VectorIdentity("a"))
      .required(t => flatten(t._2))(VectorIdentity("b"))
      .cross(("a", "b"))(_ * _)
    val f2 = FeatureSpec
      .of[A]
      .required(t => flatten(t._1))(VectorIdentity("c"))
      .required(t => flatten(t._2))(VectorIdentity("d"))
      .cross(("c", "d"))(_ + _)
    val f = MultiFeatureSpec(f1, f2).extract(xs)
    val expected =
      xs.map(x => Seq(denseValues(x, _ * _), denseValues(x, _ + _)))
    Prop.all(f.featureNames == Seq(Seq(denseNames("a", "b"), denseNames("c", "d"))),
             f.featureValues[Seq[Double]] == expected)
  }

  private implicit val labelArb = Arbitrary(Gen.alphaStr)

  type B = ((String, String), (String, String, String))
  private def flatten(v: (String, String)) = Seq(v._1, v._2)
  private def flatten(v: (String, String, String)) = Seq(v._1, v._2, v._3)

  private def sparseNames(a: String, b: String, xs: List[B]) = {
    val n1 = xs.map(t => SortedSet(flatten(t._1): _*)).reduce(_ ++ _).map(a + "_" + _).toSeq
    val n2 = xs.map(t => SortedSet(flatten(t._2): _*)).reduce(_ ++ _).map(b + "_" + _).toSeq
    n1 ++ n2 ++ (for {
      a <- n1
      b <- n2
    } yield Crossings.name(a, b))
  }
  private def sparseValues(a: String, b: String, v: B, f: (Double, Double) => Double) = {
    import scala.collection.breakOut
    val m1: Map[String, Double] =
      flatten(v._1).map(s => (a + "_" + s, 1.0))(breakOut)
    val m2: Map[String, Double] =
      flatten(v._2).map(s => (b + "_" + s, 1.0))(breakOut)
    val x: Map[String, Double] =
      (for {
        (k1, v1) <- m1
        (k2, v2) <- m2
      } yield (Crossings.name(k1, k2), f(v1, v2)))(breakOut)
    m1 ++ m2 ++ x
  }

  property("sparse") = Prop.forAll { xs: List[B] =>
    val f1 = FeatureSpec
      .of[B]
      .required(t => flatten(t._1))(NHotEncoder("a"))
      .required(t => flatten(t._2))(NHotEncoder("b"))
      .cross(("a", "b"))(_ * _)
      .extract(xs)
    val f2 = FeatureSpec
      .of[B]
      .required(t => flatten(t._1))(NHotEncoder("a"))
      .required(t => flatten(t._2))(NHotEncoder("b"))
      .cross(("a", "b"))(_ + _)
      .extract(xs)
    Prop.all(
      f1.featureNames == Seq(sparseNames("a", "b", xs)),
      f1.featureValues[Map[String, Double]] == xs.map(sparseValues("a", "b", _, _ * _)),
      f2.featureNames == Seq(sparseNames("a", "b", xs)),
      f2.featureValues[Map[String, Double]] == xs.map(sparseValues("a", "b", _, _ + _))
    )
  }

  property("sparse multi") = Prop.forAll { xs: List[B] =>
    val f1 = FeatureSpec
      .of[B]
      .required(t => flatten(t._1))(NHotEncoder("a"))
      .required(t => flatten(t._2))(NHotEncoder("b"))
      .cross(("a", "b"))(_ * _)
    val f2 = FeatureSpec
      .of[B]
      .required(t => flatten(t._1))(NHotEncoder("c"))
      .required(t => flatten(t._2))(NHotEncoder("d"))
      .cross(("c", "d"))(_ + _)
    val f = MultiFeatureSpec(f1, f2).extract(xs)
    val expected = xs.map { x =>
      Seq(sparseValues("a", "b", x, _ * _), sparseValues("c", "d", x, _ + _))
    }
    Prop.all(f.featureNames == Seq(Seq(sparseNames("a", "b", xs), sparseNames("c", "d", xs))),
             f.featureValues[Map[String, Double]] == expected)
  }

}
