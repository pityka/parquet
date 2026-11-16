package parquet

import ParquetReader.*

class ParquetSuite extends munit.FunSuite {

  private def fourTypeColumns: Seq[Column] = Seq(
    Column("i", TYPE_INT32, None, false, 5, 5, Array.empty,
      Parquet.plainI32(Array(1, 2, 3, 4, 5)), None, None),
    Column("l", TYPE_INT64, None, false, 5, 5, Array.empty,
      Parquet.plainI64(Array(10L, 20L, 30L, 40L, 50L)), None, None),
    Column("d", TYPE_DOUBLE, None, false, 5, 5, Array.empty,
      Parquet.plainF64(Array(1.5, 2.5, 3.5, 4.5, 5.5)), None, None),
    Column("s", TYPE_BYTE_ARRAY, Some(CONV_UTF8), false, 5, 5, Array.empty,
      Parquet.plainBytes(Array("a", "bb", "ccc", "dddd", "eeeee").map(_.getBytes("UTF-8"))),
      None, None)
  )

  private def assertFourTypes(read: Vector[ReadColumn]): Unit = {
    assertEquals(read.map(_.name), Vector("i", "l", "d", "s"))
    read(0) match {
      case c: ReadColumn.Ints =>
        assertEquals(c.values.toVector, Vector(1, 2, 3, 4, 5))
        assert(!c.nulls.contains(true))
      case o => fail(s"expected Ints, got $o")
    }
    read(1) match {
      case c: ReadColumn.Longs =>
        assertEquals(c.values.toVector, Vector(10L, 20L, 30L, 40L, 50L))
      case o => fail(s"expected Longs, got $o")
    }
    read(2) match {
      case c: ReadColumn.Doubles =>
        assertEquals(c.values.toVector, Vector(1.5, 2.5, 3.5, 4.5, 5.5))
      case o => fail(s"expected Doubles, got $o")
    }
    read(3) match {
      case c: ReadColumn.Strings =>
        assertEquals(c.values.toVector, Vector("a", "bb", "ccc", "dddd", "eeeee"))
      case o => fail(s"expected Strings, got $o")
    }
  }

  private def appendAsStrings(
      page: ReadColumn,
      buf: scala.collection.mutable.ArrayBuffer[String]
  ): Unit = {
    var i = 0
    while (i < page.length) {
      val s = page match {
        case _ if page.isNull(i)   => "null"
        case c: ReadColumn.Ints    => c.values(i).toString
        case c: ReadColumn.Longs   => c.values(i).toString
        case c: ReadColumn.Doubles => c.values(i).toString
        case c: ReadColumn.Strings => c.values(i)
      }
      buf += s
      i += 1
    }
  }

  test("in-memory convenience: round-trip of int32, int64, double and string (no boxing)") {
    assertFourTypes(Parquet.assembleAuto(Parquet.writeFile(fourTypeColumns, 5, CODEC_UNCOMPRESSED)))
    assertFourTypes(Parquet.assembleAuto(Parquet.writeFile(fourTypeColumns, 5, CODEC_SNAPPY)))
  }

  test("nulls round-trip through optional columns with definition levels") {
    val cols = Seq(
      Column("i", TYPE_INT32, None, true, 4, 2, Array(1, 0, 1, 0),
        Parquet.plainI32(Array(1, 3)), None, None),
      Column("s", TYPE_BYTE_ARRAY, Some(CONV_UTF8), true, 4, 3, Array(0, 1, 1, 1),
        Parquet.plainBytes(Array("y", "z", "w").map(_.getBytes("UTF-8"))), None, None)
    )
    val read = Parquet.assembleAuto(Parquet.writeFile(cols, 4, CODEC_SNAPPY))
    read(0) match {
      case c: ReadColumn.Ints =>
        assertEquals(c.nulls.toVector, Vector(false, true, false, true))
        assertEquals(c.values(0), 1); assertEquals(c.values(2), 3)
      case o => fail(s"expected Ints, got $o")
    }
    read(1) match {
      case c: ReadColumn.Strings =>
        assertEquals(c.nulls.toVector, Vector(true, false, false, false))
        assertEquals(c.values(1), "y"); assertEquals(c.values(2), "z"); assertEquals(c.values(3), "w")
      case o => fail(s"expected Strings, got $o")
    }
  }

  test("column statistics round-trip and decode via statValue") {
    val col = Column("v", TYPE_INT64, None, false, 3, 3, Array.empty,
      Parquet.plainI64(Array(5L, 1L, 9L)),
      Some(Parquet.plainI64(Array(1L))),
      Some(Parquet.plainI64(Array(9L))))
    val meta = Parquet
      .readFooter(Parquet.writeFile(Seq(col), 3, CODEC_UNCOMPRESSED))
      .rowGroups.head.head
    assertEquals(meta.minBytes.map(b => Parquet.statValue(TYPE_INT64, b)), Some(1L: Any))
    assertEquals(meta.maxBytes.map(b => Parquet.statValue(TYPE_INT64, b)), Some(9L: Any))
  }

  test("decodeChunkPages streams a column chunk page-by-page from an InputStream") {
    val col = Column("v", TYPE_INT64, None, false, 6, 6, Array.empty,
      Parquet.plainI64(Array(1L, 2L, 3L, 4L, 5L, 6L)), None, None)
    val file = Parquet.writeFile(Seq(col), 6, CODEC_UNCOMPRESSED)
    val cm = Parquet.readFooter(file).rowGroups.head.head
    val start =
      (if (cm.dictPageOffset >= 0) cm.dictPageOffset else cm.dataPageOffset).toInt
    val chunk = java.util.Arrays.copyOfRange(file, start, start + cm.totalCompressed.toInt)
    val got = scala.collection.mutable.ArrayBuffer.empty[Long]
    Parquet.decodeChunkPages(new java.io.ByteArrayInputStream(chunk), cm) {
      case c: ReadColumn.Longs =>
        var i = 0; while (i < c.values.length) { got += c.values(i); i += 1 }
      case o => fail(s"expected Longs page, got $o")
    }
    assertEquals(got.toVector, Vector(1L, 2L, 3L, 4L, 5L, 6L))
  }

  test("ParquetReader interface: write to a stream, read back via a channel, never materializing the file") {
    val reader: ParquetReader = Parquet
    val cols = Seq(
      Column("id", TYPE_INT64, None, false, 4, 4, Array.empty,
        reader.plainI64(Array(10L, 20L, 30L, 40L)), None, None),
      Column("x", TYPE_DOUBLE, None, true, 4, 3, Array(1, 1, 0, 1),
        reader.plainF64(Array(1.0, 2.0, 4.0)), None, None)
    )

    val tmp = java.nio.file.Files.createTempFile("parquet-", ".parquet")
    try {
      val os = java.nio.file.Files.newOutputStream(tmp)
      try reader.writeFile(os, cols, 4, CODEC_SNAPPY)
      finally os.close()

      val ch = java.nio.file.Files.newByteChannel(tmp, java.nio.file.StandardOpenOption.READ)
      try {
        val footer = reader.readFooter(ch)
        val rebuilt =
          scala.collection.mutable.LinkedHashMap.empty[String, scala.collection.mutable.ArrayBuffer[String]]
        footer.rowGroups.foreach { rg =>
          rg.foreach { cm =>
            val buf = rebuilt.getOrElseUpdate(cm.name, scala.collection.mutable.ArrayBuffer.empty[String])
            reader.streamColumnChunk(ch, cm)(page => appendAsStrings(page, buf))
          }
        }
        assertEquals(rebuilt("id").toVector, Vector("10", "20", "30", "40"))
        assertEquals(rebuilt("x").toVector, Vector("1.0", "2.0", "null", "4.0"))
      } finally ch.close()
    } finally java.nio.file.Files.delete(tmp)
  }
}
