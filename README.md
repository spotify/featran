featran
=======

[![Build Status](https://travis-ci.org/spotify/featran.svg?branch=master)](https://travis-ci.org/spotify/featran)
[![codecov.io](https://codecov.io/github/spotify/featran/coverage.svg?branch=master)](https://codecov.io/github/spotify/featran?branch=master)
[![GitHub license](https://img.shields.io/github/license/spotify/featran.svg)](./LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/featran-core_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/featran-core_2.11)

Featran is a Scala library for feature transformation. It supports various collection types for feature extraction and output formats for feature representation. The following artifacts are available:

- `featran-core` - Core library, supports extraction from Scala collections and output as Scala collections, [Breeze](https://github.com/scalanlp/breeze) dense and sparse vectors.
- `featran-flink` - support for extraction from [Flink](http://flink.apache.org/) `DataSet`
- `featran-scalding` - support for extraction from [Scalding](https://github.com/twitter/scalding) `TypedPipe`
- `featran-scio` - support for extraction from [Scio](https://github.com/spotify/scio) `SCollection`
- `featran-spark` - support for extraction from [Spark](https://spark.apache.org/) `RDD`
- `featran-tensorflow` - suppoprt for output as [TensorFlow](https://www.tensorflow.org/) Example Protocol Buffer
- `featran-numpy` - support for output as [NumPy](http://www.numpy.org/) `.npy` file


See [Example.scala](https://github.com/spotify/featran/blob/master/core/src/test/scala/com/spotify/featran/examples/Example.scala) for example usage.

# License

Copyright 2016-2017 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
