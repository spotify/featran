package com.spotify.featran.tensorflow

import com.spotify.featran.{CollectionType, FeatureBuilder, JsonSerializable}
import com.spotify.featran.transformers._
import org.tensorflow.example.{Example, Feature}
import shapeless.datatype.tensorflow.TensorFlowType

import scala.reflect.ClassTag

object ExampleTransformer {
  import TensorFlowType._

  def toFeature(name: String, ex: Example): Option[Feature] = {
    val fm = ex.getFeatures.getFeatureMap
    if(fm.containsKey(name)){
      Some(fm.get(name))
    } else {
      None
    }
  }

  def getDouble(name: String): Example => Option[Double] =
    (ex: Example) => toFeature(name, ex).flatMap(v => toDoubles(v).headOption)

  def getMdlRecord(name: String): Example => Option[MDLRecord[Int]] =
    (ex: Example) =>
      toFeature(name, ex).flatMap(v => toDoubles(v).headOption).map(v => MDLRecord(1, v))

  def getDoubles(name: String): Example => Option[Seq[Double]] =
    (ex: Example) => toFeature(name, ex).map(v => toDoubles(v))

  def getString(name: String): Example => Option[String] =
    (ex: Example) => toFeature(name, ex).flatMap(v => toStrings(v).headOption)

  def getStrings(name: String): Example => Option[Seq[String]] =
    (ex: Example) => toFeature(name, ex).map(v => toStrings(v))

  def apply[M[_]: CollectionType](settings: M[String]): ExampleTransformer[M] =
    new ExampleTransformer[M](settings)
}

class ExampleTransformer[M[_]: CollectionType](settings: M[String]) extends Serializable {
  import CollectionType.ops._
  import ExampleTransformer._

  private def pkg(cls: String) =
    "com.spotify.featran.transformers." + cls

  private val converters = settings.map{str =>
    val json = JsonSerializable[Seq[Settings]].decode(str).right.get
    json.map{setting =>
      val name = setting.name
      val aggr = setting.aggregators
      val params = setting.params
      setting.cls match {
        case c if c == pkg("Identity") =>
          (getDouble(name), aggr, Identity(name))
        case c if c == pkg("OneHotEncoder") =>
          (getString(name), aggr, OneHotEncoder(name))
        case c if c == pkg("NHotEncoder") =>
          (getStrings(name), aggr, NHotEncoder(name))
        case c if c == pkg("VectorIdentity") =>
          (getDoubles(name), aggr, VectorIdentity[Seq](name))
        case c if c == pkg("Binarizer") =>
          (getDouble(name), aggr, Binarizer(name, params("threshold").toDouble))
        case c if c == pkg("Bucketizer") =>
          val str = params("splits")
          val splits = str.slice(1, str.length - 1).split(",").map(_.toDouble)
          (getDouble(name), aggr, Bucketizer(name, splits))
        case c if c == pkg("HashNHotEncoder") =>
          (getStrings(name), aggr, HashNHotEncoder(name))
        case c if c == pkg("HashOneHotEncoder") =>
          (getString(name), aggr, HashOneHotEncoder(name))
        case c if c == pkg("HeavyHitters") =>
          (getString(name), aggr, HeavyHitters(name, params("heavyHittersCount").toInt))
        case c if c == pkg("MaxAbsScaler") =>
          (getDouble(name), aggr, MaxAbsScaler(name))
        case c if c == pkg("MDL") =>
          (getMdlRecord(name), aggr, MDL[Int](name))
        case c if c == pkg("MinMaxScaler") =>
          (getDouble(name), aggr, MinMaxScaler(name))
        case c if c == pkg("Normalizer") =>
          (getDouble(name), aggr, Normalizer(name))
        case c => sys.error("Unknown Transformer " + c)
      }
    }
  }

  val dimSize: M[Int] = converters.map{items =>
     items.map { case (_, aggr, tr) =>
       val ta = aggr.map(tr.decodeAggregator)
       tr.unsafeFeatureDimension(ta)
     }
    .sum
  }

  def transform[F: FeatureBuilder: ClassTag](records: M[Example]): M[F] = {
    val fb = FeatureBuilder[F].newBuilder
    records.cross(converters).cross(dimSize).map{case((record, convs), size) =>
      fb.init(size)
      convs.foreach{case(fn, aggr, conv) =>
        val a = aggr.map(conv.decodeAggregator)
        conv.unsafeBuildFeatures(fn(record), a, fb)
      }
      fb.result
    }
  }
}
