featran
=======

[![Build Status](https://travis-ci.org/spotify/featran.svg?branch=master)](https://travis-ci.org/spotify/featran)
[![codecov.io](https://codecov.io/github/spotify/featran/coverage.svg?branch=master)](https://codecov.io/github/spotify/featran?branch=master)
[![GitHub license](https://img.shields.io/github/license/spotify/featran.svg)](./LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/featran-core_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/featran-core_2.11)

Featran, also known as Featran77 or F77 (get it?), is a Scala library for feature transformation. It aims to simplify the time consuming task of feature engineering in data science and machine learning processes. It supports various collection types for feature extraction and output formats for feature representation.

# Introduction

Most feature transformation logic requires two steps, one global aggregation to summarize data followed by one element-wise mapping to transform them. For example:

- Min-Max Scaler
  - Aggregation: global min & max
  - Mapping: scale each value to `[min, max]`
- One-Hot Encoder
  - Aggregation: distinct labels
  - Mapping: convert each label to a binary vector

We can implement this in a naive way using `reduce` and `map`.

```scala
case class Point(score: Double, label: String)
val data = Seq(Point(1.0, "a"), Point(2.0, "b"), Point(3.0, "c"))

val a = data
  .map(p => (p.score, p.score, Set(p.label)))
  .reduce((x, y) => (math.min(x._1, y._1), math.max(x._2, y._2), x._3 ++ y._3))

val features = data.map { p =>
  (p.score - a._1) / (a._2 - a._1) :: a._3.toList.sorted.map(s => if (s == p.label) 1.0 else 0.0)
}
```

But this is unmanageable for complex feature sets. The above logic can be easily expressed in Featran.

```scala
import com.spotify.featran._
import com.spotify.featran.transformers._

val fs = FeatureSpec.of[Point]
  .required(_.score)(MinMaxScaler("min-max"))
  .required(_.label)(OneHotEncoder("one-hot"))

val fe = fs.extract(data)
val names = fe.featureNames
val features = fe.featureValues[Seq[Double]]
```

Featran also supports these additional features.

- Extract from Scala collections, [Flink](http://flink.apache.org/) `DataSet`s, [Scalding](https://github.com/twitter/scalding) `TypedPipe`s, [Scio](https://github.com/spotify/scio) `SCollection`s and [Spark](https://spark.apache.org/) `RDD`s
- Output as Scala collections, [Breeze](https://github.com/scalanlp/breeze) dense and sparse vectors,  [TensorFlow](https://www.tensorflow.org/) `Example` Protobuf, [XGBoost](https://github.com/dmlc/xgboost) `LabeledPoint` and [NumPy](http://www.numpy.org/) `.npy` file
- Import aggregation from a previous extraction for training, validation and test sets
- Compose feature specifications and separate outputs

See [Examples](https://spotify.github.io/featran/examples/Examples.scala.html) ([source](https://github.com/spotify/featran/blob/master/examples/src/main/scala/Examples.scala)) for detailed examples. See [transformers](https://spotify.github.io/featran/api/index.html#com.spotify.featran.transformers.package) package for a complete list of available feature transformers.

See [ScalaDocs](https://spotify.github.io/featran) for current API documentation.

# Artifacts

Feature includes the following artifacts:

- `featran-core` - core library, support for extraction from Scala collections and output as Scala collections, Breeze dense and sparse vectors
- `featran-java` - Java interface, see [JavaExample.java](https://github.com/spotify/featran/blob/master/java/src/test/java/com/spotify/featran/java/examples/JavaExample.java)
- `featran-flink` - support for extraction from Flink `DataSet`
- `featran-scalding` - support for extraction from Scalding `TypedPipe`
- `featran-scio` - support for extraction from Scio `SCollection`
- `featran-spark` - support for extraction from Spark `RDD`
- `featran-tensorflow` - support for output as TensorFlow `Example` Protobuf
- `featran-xgboost` - support for output as XGBoost `LabeledPoint`
- `featran-numpy` - support for output as NumPy `.npy` file

# License

Copyright 2016-2017 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
