package com.spotify.featran.tensorflow

import com.google.protobuf.ByteString
import org.tensorflow.proto.{example => tf}

private object TensorFlowType {

  import scala.jdk.CollectionConverters._

  def toFeature(name: String, ex: tf.Example): Option[tf.Feature] = {
    val fm = ex.getFeatures.getFeatureMap
    if (fm.containsKey(name)) {
      Some(fm.get(name))
    } else {
      None
    }
  }

  def toFloats(f: tf.Feature): Seq[Float] =
    f.getFloatList.getValueList.asScala.toSeq.asInstanceOf[Seq[Float]]

  def toDoubles(f: tf.Feature): Seq[Double] = toFloats(f).map(_.toDouble)

  def toByteStrings(f: tf.Feature): Seq[ByteString] = f.getBytesList.getValueList.asScala.toSeq

  def toStrings(f: tf.Feature): Seq[String] = toByteStrings(f).map(_.toStringUtf8)

  def fromFloats(xs: Seq[Float]): tf.Feature.Builder =
    tf.Feature
      .newBuilder()
      .setFloatList(xs.foldLeft(tf.FloatList.newBuilder())(_.addValue(_)).build())

  def fromDoubles(xs: Seq[Double]): tf.Feature.Builder = fromFloats(xs.map(_.toFloat))

  def fromByteStrings(xs: Seq[ByteString]): tf.Feature.Builder =
    tf.Feature.newBuilder().setBytesList(tf.BytesList.newBuilder().addAllValue(xs.asJava))

  def fromStrings(xs: Seq[String]): tf.Feature.Builder =
    fromByteStrings(xs.map(ByteString.copyFromUtf8))

}
