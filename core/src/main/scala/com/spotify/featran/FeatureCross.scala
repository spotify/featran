package com.spotify.featran

case class FeatureCross[T](
  fs: FeatureSet[T],
  private val cross: List[Cross],
  private val names: Array[String]) {

  private val namedFeatures: Map[String, Int] =
    fs.features.zipWithIndex.map{case(feat, idx) =>
      feat.transformer.name -> idx
    }(scala.collection.breakOut)

  {
    val dups = cross.flatMap(c => List(c.name1, c.name2)).filterNot(namedFeatures.contains)
    require(dups.isEmpty, "Can not resolve cross features: " + dups.mkString(", "))
  }

  def crossValues[F: FeatureGetter: FeatureBuilder](fg: F, index: Map[Int, Range]): F = {
    val fb = FeatureBuilder()
    val getter = implicitly[FeatureGetter[F]]

    val iter = cross.iterator
    fb.init(crossSize(index))
    while(iter.hasNext){
      values(iter.next(), fg, fb, index)
    }
    val crossed = fb.result
    getter.combine(fg, crossed)
  }

  def values[F: FeatureGetter]
  (cross: Cross, fg: F, fb: FeatureBuilder[F], index: Map[Int, Range]): Unit = {
    val getter = implicitly[FeatureGetter[F]]
    val featureIdx1 = namedFeatures(cross.name1)
    val featureIdx2 = namedFeatures(cross.name2)

    val range1 = index(featureIdx1)
    val range2 = index(featureIdx2)

    val fn = cross.combine

    var idx1 = range1.start
    var idx2 = range2.start

    range1.foreach{idx1 =>
      val name1 = names(featureIdx1)
      val v1 = getter.get(name1, idx1, fg)
      range2.foreach{idx2 =>
        val name2 = names(featureIdx2)
        val v2 = getter.get(name2, idx1, fg)
        fb.add(name1 + "-" + name2, fn(v1, v2))
      }
    }

  }

  def crossNames(index: Map[Int, Range]): Seq[String] = {
    val iter = cross.iterator
    val b = Seq.newBuilder[String]
    while(iter.hasNext){
      b ++= names(iter.next(), index)
    }
    names ++ b.result()
  }

  def names(cross: Cross, index: Map[Int, Range]): Seq[String] = {
    val featureIdx1 = namedFeatures(cross.name1)
    val featureIdx2 = namedFeatures(cross.name2)

    val range1 = index(featureIdx1)
    val range2 = index(featureIdx2)

    var idx1 = range1.start
    var idx2 = range2.start

    val b = Seq.newBuilder[String]

    range1.foreach{idx1 =>
      val name1 = names(featureIdx1)
      range2.foreach{idx2 =>
        val name2 = names(featureIdx2)
        b += name1 + "-" + name2
      }
    }

    b.result()
  }

  def crossSize(index: Map[Int, Range]): Int = {
    val iter = cross.iterator
    var sum = 0
    while(iter.hasNext){
      sum += size(iter.next(), index)
    }
    sum
  }

  def size(cross: Cross, index: Map[Int, Range]): Int = {
    val idx = namedFeatures(cross.name1)
    val crossIdx = namedFeatures(cross.name2)
    val rightDim = index(idx)
    val leftDim = index(crossIdx)

    rightDim.length * leftDim.length
  }
}
