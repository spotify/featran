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

package com.spotify.featran.jmh

import java.util.concurrent.TimeUnit

import com.spotify.featran._
import com.spotify.featran.transformers._
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class ExtractorBenchmark {

  type A = (Double, String)
  val fs: FeatureSpec[A] = FeatureSpec
    .of[A]
    .required(_._1)(StandardScaler("std"))
    .required(_._2)(OneHotEncoder("onehot"))
  val input: Seq[A] = (1 to 10).map(x => (x.toDouble, x.toString))
  val settings: Seq[String] = fs.extract(input).featureSettings
  val re: RecordExtractor[A, Seq[Double]] = fs.extractWithSettings(settings.head)

  @Benchmark def collection: Seq[Seq[Double]] =
    fs.extractWithSettings(input, settings).featureValues[Seq[Double]]
  @Benchmark def collection1: Seq[Double] =
    fs.extractWithSettings(Seq((1.0, "1.0")), settings).featureValues[Seq[Double]].head
  @Benchmark def record: Seq[Seq[Double]] = input.map(re.featureValue)
  @Benchmark def record1: Seq[Double] = re.featureValue((1.0, "1.0"))

}
