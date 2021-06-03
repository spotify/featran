package com.spotify.featran.transformers

import com.spotify.featran.{FeatureBuilder, FeatureRejection, FlatReader, FlatWriter}
import com.twitter.algebird.Aggregator

object SegmentIndices extends SettingsBuilder {

  /**
   * Create a new [[SegmentIndices]] instance.
   * @param expectedLength expected length of the input vectors, or 0 to infer from data
   */
  def apply(
             name: String,
             expectedLength: Int = 0
           ): Transformer[Array[Int], Int, Int] =
    new SegmentIndices(name, expectedLength)

  /**
   * Create a new [[SegmentIndices]] from a settings object
   * @param setting Settings object
   */
  def fromSettings(setting: Settings): Transformer[Array[Int], Int, Int] = {
    val expectedLength = setting.params("expectedLength").toInt
    SegmentIndices(setting.name, expectedLength)
  }
}

private[featran] class SegmentIndices(name: String, expectedLength: Int = 0)
  extends Transformer[Array[Int], Int, Int](name) {

  override val aggregator: Aggregator[Array[Int], Int, Int] =
    Aggregators.seqLength(expectedLength)
  override def featureDimension(c: Int): Int = c
  override def featureNames(c: Int): Seq[String] = names(c)

  override def buildFeatures(a: Option[Array[Int]], c: Int, fb: FeatureBuilder[_]): Unit = a match {
    //TODO: Require increasing input (non-strict monotonic)
    case Some(x) if (x.length != c) =>
      fb.skip(c)
      fb.reject(this, FeatureRejection.WrongDimension(c, x.length))
    case Some(x) => {
      val copyOfX = x.clone()

      var tmp: Int = 0
      copyOfX(0) = 0 //TODO: Set first element to 0, Guard against head being empty or non-zero

      for (index <- 1 until copyOfX.length) { //Skip 0th!
        val inputValue = copyOfX(index)

        if (inputValue == tmp)
          copyOfX(index) = copyOfX(index - 1) + 1
        else
          copyOfX(index) = 0
        tmp = inputValue
      }

      fb.addInts(names = names(c), values = copyOfX)
    }
    case None => fb.skip(c)
  }

  override def encodeAggregator(c: Int): String = c.toString
  override def decodeAggregator(s: String): Int = s.toInt
  override def params: Map[String, String] =
    Map("expectedLength" -> expectedLength.toString)

  override def flatRead[T: FlatReader]: T => Option[Any] = FlatReader[T].readIntArray(name)

  override def flatWriter[T](implicit fw: FlatWriter[T]): Option[Array[Int]] => fw.IF = fw.writeIntArray(name)

}
