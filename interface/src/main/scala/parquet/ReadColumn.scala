package parquet

sealed abstract class ReadColumn {
  def name: String
  def ptype: Int
  def convertedType: Int
  def length: Int
  def nulls: Array[Boolean]
  final def isNull(i: Int): Boolean = nulls(i)
}

object ReadColumn {

  final case class Ints(
      name: String,
      convertedType: Int,
      values: Array[Int],
      nulls: Array[Boolean]
  ) extends ReadColumn {
    def ptype: Int = ParquetReader.TYPE_INT32
    def length: Int = values.length
  }

  final case class Longs(
      name: String,
      convertedType: Int,
      values: Array[Long],
      nulls: Array[Boolean]
  ) extends ReadColumn {
    def ptype: Int = ParquetReader.TYPE_INT64
    def length: Int = values.length
  }

  final case class Doubles(
      name: String,
      ptype: Int,
      convertedType: Int,
      values: Array[Double],
      nulls: Array[Boolean]
  ) extends ReadColumn {
    def length: Int = values.length
  }

  final case class Strings(
      name: String,
      convertedType: Int,
      values: Array[String],
      nulls: Array[Boolean]
  ) extends ReadColumn {
    def ptype: Int = ParquetReader.TYPE_BYTE_ARRAY
    def length: Int = values.length
  }
}
