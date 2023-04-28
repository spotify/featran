package com.spotify.featran.tensorflow

import org.tensorflow.proto.{example => tf}

import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

final case class NamedTFFeature(name: String, f: tf.Feature)

private object FeatureNameNormalization {
  private val NamePattern = Pattern.compile("[^A-Za-z0-9_]")

  val normalize: String => String = {
    lazy val cache = new ConcurrentHashMap[String, String]()
    fn =>
      cache.computeIfAbsent(
        fn,
        (n: String) => NamePattern.matcher(n).replaceAll("_")
      )
  }
}
