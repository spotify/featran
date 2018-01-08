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

import com.spotify.featran.{FeatureExtractor, MultiFeatureExtractor}
import com.spotify.scio.io.Tap
import com.spotify.scio.tensorflow._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.Compression
import org.tensorflow.{example => tf}
import scala.concurrent.Future

private object FeatranTFRecordSpec {

  def fromFeatureSpec(featureNames: SCollection[Seq[String]]): TFRecordSpec = {
    TFRecordSpec.fromSColSeqFeatureInfo(
      featureNames.map(_.map(n => FeatureInfo(n, FeatureKind.FloatList, Map()))))
  }

  def fromMultiSpec(featureNames: SCollection[Seq[Seq[String]]]): TFRecordSpec = {
    val featureInfos = featureNames.map(_.zipWithIndex.flatMap {
      case (sss, i) => sss.map(n =>
        FeatureInfo(n, FeatureKind.FloatList, Map("multispec-id" -> i.toString)))
    })
    TFRecordSpec.fromSColSeqFeatureInfo(featureInfos)
  }
}

private[featran] class TFExampleSCollectionFunctions[T <: tf.Example](val self: SCollection[T]) {

  import com.spotify.scio.tensorflow._

  /**
   *
   * Save this SCollection of [[tf.Example]] as TensorFlow TFRecord files.
   * @param fe FeatureExtractor, obtained from Featran after calling extract on a
   *           [[com.spotify.featran.FeatureSpec]]
   * @group output
   */
  def saveAsTfExampleFile(path: String,
                          fe: FeatureExtractor[SCollection, _]):
  (Future[Tap[tf.Example]], Future[Tap[String]]) = {
    saveAsTfExampleFile(path, fe, Compression.UNCOMPRESSED)
  }

  /**
   * Save this SCollection of [[tf.Example]] as TensorFlow TFRecord files.
   *
   * @param fe FeatureExtractor, obtained from Featran after calling extract on a
   *           [[com.spotify.featran.FeatureSpec]]
   * @group output
   */
  def saveAsTfExampleFile(path: String,
                          fe: FeatureExtractor[SCollection, _],
                          compression: Compression):
  (Future[Tap[tf.Example]], Future[Tap[String]]) = {
    self.saveAsTfExampleFile(path,
      FeatranTFRecordSpec.fromFeatureSpec(fe.featureNames),
      compression = compression)
  }

}

private[featran] class SeqTFExampleSCollectionFunctions[T <: tf.Example]
(@transient val self: SCollection[Seq[T]]) extends Serializable {

  import com.spotify.scio.tensorflow._

  def mergeExamples(e: Seq[tf.Example]): tf.Example = e
    .foldLeft(tf.Example.newBuilder)((b, i) => b.mergeFrom(i))
    .build()

  /**
   * Merge each [[Seq]] of [[tf.Example]] and save them as TensorFlow TFRecord files.
   * Caveat: If some feature names are repeated in different feature specs, they will be collapsed.
   *
   * @param fe FeatureExtractor, obtained from Featran after calling extract on a
   *           [[com.spotify.featran.MultiFeatureSpec]]
   * @group output
   */
  def saveAsTfExampleFile(path: String,
                          fe: MultiFeatureExtractor[SCollection, _]):
  (Future[Tap[tf.Example]], Future[Tap[String]]) = {
    saveAsTfExampleFile(path, fe, Compression.UNCOMPRESSED)
  }

  /**
   * Merge each [[Seq]] of [[tf.Example]] and save them as TensorFlow TFRecord files.
   * Caveat: If some feature names are repeated in different feature specs, they will be collapsed.
   *
   * @param fe FeatureExtractor, obtained from Featran after calling extract on a
   *           [[com.spotify.featran.MultiFeatureSpec]]
   * @group output
   */
  def saveAsTfExampleFile(path: String,
                          fe: MultiFeatureExtractor[SCollection, _],
                          compression: Compression):
  (Future[Tap[tf.Example]], Future[Tap[String]]) = {
    self.map(mergeExamples)
      .saveAsTfExampleFile(path,
        FeatranTFRecordSpec.fromMultiSpec(fe.featureNames),
        compression = compression)
  }

}
