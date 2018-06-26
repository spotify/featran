package com.spotify.featran.converters

import com.spotify.featran.transformers.{Identity, Transformer}

/**
 * Default Type Class used by the from generator for Case Class Conversions
 */
trait DefaultTransform[T] {
  def apply(featureName: String): Transformer[T, _, _]
}

case object IdentityDefault extends DefaultTransform[Double] {
  def apply(featureName: String): Transformer[Double, _, _] = Identity(featureName)
}
