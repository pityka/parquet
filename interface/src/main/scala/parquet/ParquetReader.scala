package parquet

import java.io.{InputStream, OutputStream}
import java.nio.channels.SeekableByteChannel

trait ParquetReader {

  def readFooter(source: SeekableByteChannel): Footer

  def streamColumnChunk(source: SeekableByteChannel, cm: PColMeta)(
      onPage: ReadColumn => Unit
  ): Unit

  def decodeChunkPages(in: InputStream, cm: PColMeta)(
      onPage: ReadColumn => Unit
  ): Unit

  def statValue(ptype: Int, bytes: Array[Byte]): Any

  def writeFile(out: OutputStream, cols: Seq[Column], numRows: Int, codec: Int): Unit

  def plainI32(d: Array[Int]): Array[Byte]

  def plainI64(d: Array[Long]): Array[Byte]

  def plainF64(d: Array[Double]): Array[Byte]

  def plainBytes(d: Array[Array[Byte]]): Array[Byte]
}

object ParquetReader {
  val TYPE_INT32 = 1
  val TYPE_INT64 = 2
  val TYPE_FLOAT = 4
  val TYPE_DOUBLE = 5
  val TYPE_BYTE_ARRAY = 6

  val CONV_UTF8 = 0
  val CONV_TIMESTAMP_MILLIS = 9

  val CODEC_UNCOMPRESSED = 0
  val CODEC_SNAPPY = 1
  val CODEC_GZIP = 2
  val CODEC_ZSTD = 6
}
