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

package com.spotify.featran.converters

import com.spotify.featran.transformers.{MDLRecord, WeightedLabel}
import org.scalatest.{FlatSpec, Matchers}

case class TestData(
  num: Int,
  str: String,
  d: Double,
  l: Long,
  s: List[String],
  b: Boolean
)

case class TestOpt(num: Option[Int])

case class TestDataOpt(num: Option[Int], d: Option[Double])

case class TestAllNatives(
  i: Int = 1,
  s: Short = 1,
  l: Long = 1L,
  d: Double = 1.0,
  io: Option[Int] = Some(1),
  so: Option[Short] = Some(1),
  lo: Option[Long] = Some(1L),
  dopt: Option[Double] = Some(1.0),
  il: List[Int] = List(1),
  sl: List[Short] = List(1),
  ll: List[Long] = List(1L),
  dl: List[Double] = List(1.0)
)

case class TestObjects(
  str: String = "a",
  strs: List[String] = List("a"),
  mdl: MDLRecord[String] = MDLRecord("a", 1.0),
  we: List[WeightedLabel] = List(WeightedLabel("a", 1.0))
)

class CaseClassConverterTest extends FlatSpec with Matchers {
  it should "convert a case class to a spec" in {
    val data = List(
      TestData(1, "a", 1.0, 1L, List("c"), b = true),
      TestData(2, "b", 1.0, 1L, List("d"), b = true)
    )

    val spec = CaseClassConverter.toSpec[TestData]
    val features = spec.extract(data).featureValues[Seq[Double]]
    assert(
      features === List(Seq(1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0),
                        Seq(2.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0)))
  }

  it should "convert a simple option" in {
    val data = List(
      TestOpt(Some(1)),
      TestOpt(None)
    )

    val spec = CaseClassConverter.toSpec[TestOpt]
    val features = spec.extract(data).featureValues[Seq[Double]]
    assert(features === List(Seq(1.0), Seq(0.0)))
  }

  it should "convert a case class to a spec with optionals" in {
    val data = List(
      TestDataOpt(Some(1), Some(1.0)),
      TestDataOpt(None, None)
    )

    val spec = CaseClassConverter.toSpec[TestDataOpt]
    val features = spec.extract(data).featureValues[Seq[Double]]
    assert(features === List(Seq(1.0, 1.0), Seq(0.0, 0.0)))
  }

  it should "test all native types" in {
    val data = List(TestAllNatives())

    val spec = CaseClassConverter.toSpec[TestAllNatives]
    val features = spec.extract(data).featureValues[Seq[Double]]
    assert(features === List(0.until(12).toList.map(_ => 1.0)))
  }

  it should "test all object types" in {
    val data = List(TestObjects())

    val spec = CaseClassConverter.toSpec[TestObjects]
    val features = spec.extract(data).featureValues[Seq[Double]]
    assert(features === List(0.until(4).toList.map(_ => 1.0)))
  }
}
