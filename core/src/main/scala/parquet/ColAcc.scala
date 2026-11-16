package parquet

import scala.collection.mutable.ArrayBuilder

private[parquet] sealed trait ColAcc {
  def addNull(): Unit
  def addPlain(dr: CReader): Unit
  def loadDict(dr: CReader, count: Int): Unit
  def addDict(ix: Int): Unit
  def addFrom(col: ReadColumn, i: Int): Unit
  def build(name: String, convertedType: Int): ReadColumn
  def emitReset(name: String, convertedType: Int): ReadColumn
}

private[parquet] object ColAcc {

  def of(ptype: Int): ColAcc = ptype match {
    case ParquetReader.TYPE_INT32      => new IntAcc
    case ParquetReader.TYPE_INT64      => new LongAcc
    case ParquetReader.TYPE_FLOAT      => new DoubleAcc(ParquetReader.TYPE_FLOAT)
    case ParquetReader.TYPE_DOUBLE     => new DoubleAcc(ParquetReader.TYPE_DOUBLE)
    case ParquetReader.TYPE_BYTE_ARRAY => new StringAcc
    case other =>
      throw new RuntimeException(s"unsupported physical type $other")
  }

  private def readInt(dr: CReader): Int = { val v = dr.le32At(dr.pos); dr.pos += 4; v }
  private def readLong(dr: CReader): Long = {
    var v = 0L; var k = 0
    while (k < 8) { v |= (dr.data(dr.pos) & 0xffL) << (8 * k); dr.pos += 1; k += 1 }
    v
  }
  private def readFloat(dr: CReader): Double = {
    var bits = 0; var k = 0
    while (k < 4) { bits |= (dr.data(dr.pos) & 0xff) << (8 * k); dr.pos += 1; k += 1 }
    java.lang.Float.intBitsToFloat(bits).toDouble
  }
  private def readDouble(dr: CReader): Double = {
    var bits = 0L; var k = 0
    while (k < 8) { bits |= (dr.data(dr.pos) & 0xffL) << (8 * k); dr.pos += 1; k += 1 }
    java.lang.Double.longBitsToDouble(bits)
  }
  private def readString(dr: CReader): String = {
    val len = dr.le32At(dr.pos); dr.pos += 4
    val a = java.util.Arrays.copyOfRange(dr.data, dr.pos, dr.pos + len)
    dr.pos += len; new String(a, "UTF-8")
  }

  final class IntAcc extends ColAcc {
    private var vals = new ArrayBuilder.ofInt
    private var nul = new ArrayBuilder.ofBoolean
    private var dict: Array[Int] = null
    def addNull(): Unit = { vals += 0; nul += true }
    def addPlain(dr: CReader): Unit = { vals += readInt(dr); nul += false }
    def loadDict(dr: CReader, count: Int): Unit = {
      dict = new Array[Int](count); var i = 0
      while (i < count) { dict(i) = readInt(dr); i += 1 }
    }
    def addDict(ix: Int): Unit = { vals += dict(ix); nul += false }
    def addFrom(col: ReadColumn, i: Int): Unit = col match {
      case c: ReadColumn.Ints =>
        if (c.nulls(i)) addNull() else { vals += c.values(i); nul += false }
      case _ => throw new RuntimeException("type mismatch merging an int column")
    }
    def build(name: String, convertedType: Int): ReadColumn =
      ReadColumn.Ints(name, convertedType, vals.result(), nul.result())
    def emitReset(name: String, convertedType: Int): ReadColumn = {
      val c = build(name, convertedType)
      vals = new ArrayBuilder.ofInt; nul = new ArrayBuilder.ofBoolean
      c
    }
  }

  final class LongAcc extends ColAcc {
    private var vals = new ArrayBuilder.ofLong
    private var nul = new ArrayBuilder.ofBoolean
    private var dict: Array[Long] = null
    def addNull(): Unit = { vals += 0L; nul += true }
    def addPlain(dr: CReader): Unit = { vals += readLong(dr); nul += false }
    def loadDict(dr: CReader, count: Int): Unit = {
      dict = new Array[Long](count); var i = 0
      while (i < count) { dict(i) = readLong(dr); i += 1 }
    }
    def addDict(ix: Int): Unit = { vals += dict(ix); nul += false }
    def addFrom(col: ReadColumn, i: Int): Unit = col match {
      case c: ReadColumn.Longs =>
        if (c.nulls(i)) addNull() else { vals += c.values(i); nul += false }
      case _ => throw new RuntimeException("type mismatch merging a long column")
    }
    def build(name: String, convertedType: Int): ReadColumn =
      ReadColumn.Longs(name, convertedType, vals.result(), nul.result())
    def emitReset(name: String, convertedType: Int): ReadColumn = {
      val c = build(name, convertedType)
      vals = new ArrayBuilder.ofLong; nul = new ArrayBuilder.ofBoolean
      c
    }
  }

  final class DoubleAcc(ptype: Int) extends ColAcc {
    private var vals = new ArrayBuilder.ofDouble
    private var nul = new ArrayBuilder.ofBoolean
    private var dict: Array[Double] = null
    private def readOne(dr: CReader): Double =
      if (ptype == ParquetReader.TYPE_FLOAT) readFloat(dr) else readDouble(dr)
    def addNull(): Unit = { vals += 0.0; nul += true }
    def addPlain(dr: CReader): Unit = { vals += readOne(dr); nul += false }
    def loadDict(dr: CReader, count: Int): Unit = {
      dict = new Array[Double](count); var i = 0
      while (i < count) { dict(i) = readOne(dr); i += 1 }
    }
    def addDict(ix: Int): Unit = { vals += dict(ix); nul += false }
    def addFrom(col: ReadColumn, i: Int): Unit = col match {
      case c: ReadColumn.Doubles =>
        if (c.nulls(i)) addNull() else { vals += c.values(i); nul += false }
      case _ => throw new RuntimeException("type mismatch merging a double column")
    }
    def build(name: String, convertedType: Int): ReadColumn =
      ReadColumn.Doubles(name, ptype, convertedType, vals.result(), nul.result())
    def emitReset(name: String, convertedType: Int): ReadColumn = {
      val c = build(name, convertedType)
      vals = new ArrayBuilder.ofDouble; nul = new ArrayBuilder.ofBoolean
      c
    }
  }

  final class StringAcc extends ColAcc {
    private var vals = ArrayBuilder.make[String]
    private var nul = new ArrayBuilder.ofBoolean
    private var dict: Array[String] = null
    def addNull(): Unit = { vals += null; nul += true }
    def addPlain(dr: CReader): Unit = { vals += readString(dr); nul += false }
    def loadDict(dr: CReader, count: Int): Unit = {
      dict = new Array[String](count); var i = 0
      while (i < count) { dict(i) = readString(dr); i += 1 }
    }
    def addDict(ix: Int): Unit = { vals += dict(ix); nul += false }
    def addFrom(col: ReadColumn, i: Int): Unit = col match {
      case c: ReadColumn.Strings =>
        if (c.nulls(i)) addNull() else { vals += c.values(i); nul += false }
      case _ => throw new RuntimeException("type mismatch merging a string column")
    }
    def build(name: String, convertedType: Int): ReadColumn =
      ReadColumn.Strings(name, convertedType, vals.result(), nul.result())
    def emitReset(name: String, convertedType: Int): ReadColumn = {
      val c = build(name, convertedType)
      vals = ArrayBuilder.make[String]; nul = new ArrayBuilder.ofBoolean
      c
    }
  }
}
