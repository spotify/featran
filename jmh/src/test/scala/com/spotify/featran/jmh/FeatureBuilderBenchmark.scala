package com.spotify.featran.jmh

import java.util.concurrent.TimeUnit

import breeze.linalg._
import com.spotify.featran._
import com.spotify.featran.tensorflow._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import org.tensorflow.example.Example

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class FeatureBuilderBenchmark {

  private val names = (750 until 1000).map(_.toString)
  private val values = (750 until 1000).map(_.toDouble)

  def benchmark[T: FeatureBuilder](bh: Blackhole): Unit = {
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
    bh.consume(fb.result)
  }

  @Benchmark def array(bh: Blackhole): Unit = benchmark[Array[Double]](bh)
  @Benchmark def seq(bh: Blackhole): Unit = benchmark[Seq[Double]](bh)
  @Benchmark def sparseArray(bh: Blackhole): Unit = benchmark[SparseArray[Double]](bh)
  @Benchmark def denseVector(bh: Blackhole): Unit = benchmark[DenseVector[Double]](bh)
  @Benchmark def sparseVector(bh: Blackhole): Unit = benchmark[SparseVector[Double]](bh)
  @Benchmark def map(bh: Blackhole): Unit = benchmark[Map[String, Double]](bh)
  @Benchmark def tensorflow(bh: Blackhole): Unit = benchmark[Example](bh)

}
