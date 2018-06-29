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

package com.spotify.featran.scio

import com.spotify.featran._
import com.spotify.featran.transformers._
import com.spotify.scio.testing._

object DummyConverter {
  import scala.reflect.runtime.universe._
  implicit val tfConverter: Converter[String] = new Converter[String] {
    type RT = String

    def apply[A, B](name: String, typ: Type, fn: A => Option[B]): A => String = { (v: A) =>
      s"$name"
    }

    def convert[T](row: T, fns: ConvertFns[T, String]): String =
      fns.fns
        .map { f =>
          f(row)
        }
        .mkString(",")
  }
}

class ScioTest extends PipelineSpec {
  import DummyConverter._
  import Fixtures._

  "Scio" should "work with FeatureSpec" in {
    runWithContext { sc =>
      val f = testSpec.extract(sc.parallelize(testData))
      testSpec.convert(sc.parallelize(testData))
      f.featureNames should containSingleValue(expectedNames)
      f.featureValues[Seq[Double]] should containInAnyOrder(expectedValues)
    }
  }

  it should "work with MultiFeatureSpec" in {
    noException shouldBe thrownBy {
      runWithContext { sc =>
        val f = recordSpec.extract(sc.parallelize(records))
        recordSpec.convert(sc.parallelize(records))
        f.featureNames
        f.featureValues[Seq[Double]]
      }
    }
  }

  private class NonSerializable {
    def method(a: String): Double = a.toDouble
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail on serialization error" in {
    an[Exception] should be thrownBy {
      runWithContext { sc =>
        val foo = new NonSerializable()
        val f = FeatureSpec
          .of[(String, Int)]
          .required(e => foo.method(e._1))(Identity("foo"))
          .extract(sc.parallelize(testData))

        f.featureValues[Seq[Double]]
      }
    }
  }
  // scalastyle:on no.whitespace.before.left.bracket

}
