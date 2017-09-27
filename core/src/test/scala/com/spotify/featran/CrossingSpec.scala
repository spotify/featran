package com.spotify.featran

import com.spotify.featran.transformers.{NHotEncoder, VectorIdentity}
import org.scalacheck._

import scala.collection.SortedSet

object CrossingSpec extends Properties("CrossingSpec") {

  implicit def arbList[T: Arbitrary]: Arbitrary[List[T]] =
    Arbitrary(Gen.listOfN(100, Arbitrary.arbitrary[T]))

  implicit val arbDouble: Arbitrary[Double] = Arbitrary(Gen.choose(-1.0, 1.0))

  type A = ((Double, Double), (Double, Double, Double))
  private def flatten(v: (Double, Double)) = Array(v._1, v._2)
  private def flatten(v: (Double, Double, Double)) = Array(v._1, v._2, v._3)

  private val denseNames = Seq(
    "a_0", "a_1", "b_0", "b_1", "b_2",
    "cross_a_0_x_b_0", "cross_a_0_x_b_1", "cross_a_0_x_b_2",
    "cross_a_1_x_b_0", "cross_a_1_x_b_1", "cross_a_1_x_b_2")
  private def denseValues(v: A, f: (Double, Double) => Double) = Seq(
    v._1._1, v._1._2, v._2._1, v._2._2, v._2._3,
    f(v._1._1, v._2._1), f(v._1._1, v._2._2), f(v._1._1, v._2._3),
    f(v._1._2, v._2._1), f(v._1._2, v._2._2), f(v._1._2, v._2._3))

  property("dense") = Prop.forAll { xs: List[A] =>
    val f1 = FeatureSpec.of[A]
      .required(t => flatten(t._1))(VectorIdentity("a"))
      .required(t => flatten(t._2))(VectorIdentity("b"))
      .cross(("a", "b"))(_ * _)
      .extract(xs)
    val f2 = FeatureSpec.of[A]
      .required(t => flatten(t._1))(VectorIdentity("a"))
      .required(t => flatten(t._2))(VectorIdentity("b"))
      .cross(("a", "b"))(_ + _)
      .extract(xs)
    Prop.all(
      f1.featureNames == Seq(denseNames),
      f1.featureValues[Seq[Double]] == xs.map(denseValues(_, _ * _)),
      f2.featureNames == Seq(denseNames),
      f2.featureValues[Seq[Double]] == xs.map(denseValues(_, _ + _)))
  }

  private implicit val labelArb = Arbitrary(Gen.alphaStr)

  type B = ((String, String), (String, String, String))
  private def flatten(v: (String, String)) = Seq(v._1, v._2)
  private def flatten(v: (String, String, String)) = Seq(v._1, v._2, v._3)

  private def sparseValues(v: B, f: (Double, Double) => Double) = {
    import scala.collection.breakOut
    val m1: Map[String, Double] = flatten(v._1).map(s => ("a_" + s, 1.0))(breakOut)
    val m2: Map[String, Double] = flatten(v._2).map(s => ("b_" + s, 1.0))(breakOut)
    val x: Map[String, Double] =
      (for ((k1, v1) <- m1; (k2, v2) <- m2) yield (Crossings.name(k1, k2), f(v1, v2)))(breakOut)
    m1 ++ m2 ++ x
  }

  property("sparse") = Prop.forAll { xs: List[B] =>
    val f1 = FeatureSpec.of[B]
      .required(t => flatten(t._1))(NHotEncoder("a"))
      .required(t => flatten(t._2))(NHotEncoder("b"))
      .cross("a", "b")(_ * _)
      .extract(xs)
    val f2 = FeatureSpec.of[B]
      .required(t => flatten(t._1))(NHotEncoder("a"))
      .required(t => flatten(t._2))(NHotEncoder("b"))
      .cross("a", "b")(_ + _)
      .extract(xs)
    val n1 = xs.map(t => SortedSet(flatten(t._1): _*)).reduce(_ ++ _).map("a_" + _).toSeq
    val n2 = xs.map(t => SortedSet(flatten(t._2): _*)).reduce(_ ++ _).map("b_" + _).toSeq
    val names = n1 ++ n2 ++ (for (a <- n1; b <- n2) yield Crossings.name(a, b))
    Prop.all(
      f1.featureNames == Seq(names),
      f1.featureValues[Map[String, Double]] == xs.map(sparseValues(_, _ * _)),
      f2.featureNames == Seq(names),
      f2.featureValues[Map[String, Double]] == xs.map(sparseValues(_, _ + _)))
  }

}
