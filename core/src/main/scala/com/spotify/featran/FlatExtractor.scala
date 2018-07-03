package com.spotify.featran

import com.spotify.featran.transformers.{MDLRecord, Settings, SettingsBuilder, WeightedLabel}
import simulacrum.typeclass

import scala.reflect.ClassTag

/**
 * TypeClass that is used to read data from flat files.  The requirement is that each
 * feature comes from the same type and can be looked up by name.
 * @tparam T
 */
@typeclass trait FlatReader[T] extends Serializable {
  def getDouble(name: String): T => Option[Double]

  def getMdlRecord(name: String): T => Option[MDLRecord[String]]

  def getWeightedLabel(name: String): T => Option[List[WeightedLabel]]

  def getDoubles(name: String): T => Option[Seq[Double]]

  def getDoubleArray(name: String): T => Option[Array[Double]]

  def getString(name: String): T => Option[String]

  def getStrings(name: String): T => Option[Seq[String]]
}

object FlatExtractor {
  def apply[M[_]: CollectionType, T: ClassTag: FlatReader](setCol: M[String]): FlatExtractor[M, T] =
    new FlatExtractor[M, T](setCol)
}

/**
 * Sometimes it is useful to store the features in an intermediate state in normally
 * a flat version like Examples or maybe JSON.  This makes it easier to interface with
 * other systems.
 *
 * This function allows the reading of data from these flat versions by name with a given
 * settings file to extract the final output.
 *
 * @param settings Settings file used to read and transform the data
 * @tparam M Collection Type
 * @tparam T The intermediate storage format for each feature.
 */
class FlatExtractor[M[_]: CollectionType, T: ClassTag: FlatReader](settings: M[String])
    extends Serializable {

  import CollectionType.ops._
  import scala.reflect.runtime.universe

  private def cName[T: ClassTag]: String =
    implicitly[ClassTag[T]].runtimeClass.getCanonicalName

  @transient private val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

  private val converters = settings.map { str =>
    val jsonOpt = JsonSerializable[Seq[Settings]].decode(str)
    assert(jsonOpt.isRight, "Unable to parse the settings files.")
    jsonOpt.right.get.map { setting =>
      val transformer = runtimeMirror
        .reflectModule(runtimeMirror.staticModule(setting.cls))
        .instance
        .asInstanceOf[SettingsBuilder]
        .fromSetting(setting)

      (transformer.flatRead[T], setting.aggregators, transformer)
    }
  }

  private val dimSize: M[Int] = converters.map { items =>
    items.map {
      case (_, aggr, tr) =>
        val ta = aggr.map(tr.decodeAggregator)
        tr.unsafeFeatureDimension(ta)
    }.sum
  }

  def run[F: FeatureBuilder: ClassTag](records: M[T]): M[F] = {
    val fb = FeatureBuilder[F].newBuilder
    records.cross(converters).cross(dimSize).map {
      case ((record, convs), size) =>
        fb.init(size)
        convs.foreach {
          case (fn, aggr, conv) =>
            val a = aggr.map(conv.decodeAggregator)
            conv.unsafeBuildFeatures(fn(record), a, fb)
        }
        fb.result
    }
  }
}
