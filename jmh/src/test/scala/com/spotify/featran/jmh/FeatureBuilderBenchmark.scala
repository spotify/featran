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

import breeze.linalg._
import com.spotify.featran._
import com.spotify.featran.tensorflow._
import org.openjdk.jmh.annotations._
import org.tensorflow.example.Example

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class FeatureBuilderBenchmark {

  private val names = (750 until 1000).map(_.toString)
  private val values = (750 until 1000).map(_.toDouble)

  def benchmark[T: FeatureBuilder]: T = {
    val fb = FeatureBuilder[T]
    fb.init(1000)
    var i = 0
    while (i < 500) {
      fb.add(i.toString, i.toDouble)
      fb.skip()
      i += 2
    }
    fb.skip(250)
    fb.add(names, values)
    fb.result
  }

  @Benchmark def array: Unit = benchmark[Array[Double]]
  @Benchmark def seq: Unit = benchmark[Seq[Double]]
  @Benchmark def sparseArray: Unit = benchmark[SparseArray[Double]]
  @Benchmark def denseVector: Unit = benchmark[DenseVector[Double]]
  @Benchmark def sparseVector: Unit = benchmark[SparseVector[Double]]
  @Benchmark def map: Unit = benchmark[Map[String, Double]]
  @Benchmark def tensorflow: Unit = benchmark[Example]

}
