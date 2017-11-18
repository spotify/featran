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
    val fb = implicitly[FeatureBuilder[T]]
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
