# parquet

A single, dependency-light, hand-rolled Parquet reader (and writer) for Scala 3,
extracted so that the `ra3` (relational3) and `bten` projects can both depend on one
shared implementation behind a common interface, instead of each vendoring their own
copy.

Published to GitHub Packages at `https://maven.pkg.github.com/pityka/parquet`.

## Modules

| Module | Artifact | Depends on | Contents |
| --- | --- | --- | --- |
| `interface` | `parquet-interface` | — | The `ParquetReader` contract, its constants, and the data records. No external dependencies. |
| `core` | `parquet-core` | `parquet-interface`, `io.airlift:aircompressor` | The one implementation: `object Parquet extends ParquetReader`. |

Both modules use package `parquet`. A consumer depends on `parquet-core` for the
concrete reader (and `parquet-interface` transitively), or on `parquet-interface` alone
to compile against the abstraction.

## Decoded columns do not box

`ReadColumn` is a typed ADT backed by primitive arrays plus a null mask — never
`Vector[Option[Any]]`:

```scala
sealed abstract class ReadColumn { def name: String; def ptype: Int; def convertedType: Int
                                   def length: Int; def nulls: Array[Boolean] }
object ReadColumn {
  final case class Ints   (name: String, convertedType: Int,            values: Array[Int],    nulls: Array[Boolean]) extends ReadColumn
  final case class Longs  (name: String, convertedType: Int,            values: Array[Long],   nulls: Array[Boolean]) extends ReadColumn
  final case class Doubles(name: String, ptype: Int, convertedType: Int, values: Array[Double], nulls: Array[Boolean]) extends ReadColumn
  final case class Strings(name: String, convertedType: Int,            values: Array[String], nulls: Array[Boolean]) extends ReadColumn
}
```

`values` is dense, length `== length`; `nulls(i)` is `true` where the row is null. The
decoder fills primitive `ArrayBuilder`s directly, so no value is ever boxed. `FLOAT` is
widened into a `Doubles` column.

## The interface never takes or returns a whole-file array

A whole-file `Array[Byte]` is `Int`-indexed, so capped near 2 GB — and real files, and
even single column chunks, exceed that. So the interface takes a seekable channel for
reading and an output stream for writing, and works one bounded unit at a time:

```scala
trait ParquetReader {
  def readFooter(source: java.nio.channels.SeekableByteChannel): Footer
  def streamColumnChunk(source: java.nio.channels.SeekableByteChannel, cm: PColMeta)(onPage: ReadColumn => Unit): Unit
  def decodeChunkPages(in: java.io.InputStream, cm: PColMeta)(onPage: ReadColumn => Unit): Unit
  def statValue(ptype: Int, bytes: Array[Byte]): Any
  def writeFile(out: java.io.OutputStream, cols: Seq[Column], numRows: Int, codec: Int): Unit
  def plainI32(d: Array[Int]): Array[Byte]
  def plainI64(d: Array[Long]): Array[Byte]
  def plainF64(d: Array[Double]): Array[Byte]
  def plainBytes(d: Array[Array[Byte]]): Array[Byte]
}
```

- `readFooter(channel)` reads the footer **from the tail**: it seeks to the end, reads
  the last 8 bytes (the 4-byte little-endian footer length, then `PAR1`), then reads
  only the `length`-byte footer. The file is never read into memory to obtain the schema
  and each chunk's `PColMeta`; the offsets in `PColMeta` are absolute file offsets.
- `streamColumnChunk(channel, cm)(onPage)` seeks to that chunk and decodes it **page by
  page**, emitting one bounded `ReadColumn` per data page. A chunk larger than 2 GB (or a
  column with more than `Int.MaxValue` rows) is processed in page-sized pieces; nothing
  large is allocated. `decodeChunkPages(in, cm)` is the same over any `InputStream`.
- `writeFile(out, …)` streams the file to an `OutputStream`.

`object ParquetReader` holds the constants `TYPE_INT32/INT64/FLOAT/DOUBLE/BYTE_ARRAY`,
`CONV_UTF8/TIMESTAMP_MILLIS`, `CODEC_UNCOMPRESSED/SNAPPY/GZIP/ZSTD`. The records
`ReadColumn`, `Column`, `PColMeta`, `SchemaRaw`, `Footer` are the shared vocabulary.

### Reading a large file

```scala
import parquet.*
import java.nio.file.*

val reader: ParquetReader = Parquet
val ch = Files.newByteChannel(path, StandardOpenOption.READ)
try {
  val footer = reader.readFooter(ch)                // tail only; no full read
  footer.rowGroups.foreach { rg =>
    rg.foreach { cm =>                               // cm: PColMeta, from the footer
      reader.streamColumnChunk(ch, cm) { page =>     // one ReadColumn per data page
        page match {
          case c: ReadColumn.Longs   => // c.values: Array[Long], c.nulls: Array[Boolean]
          case c: ReadColumn.Doubles =>
          case c: ReadColumn.Ints    =>
          case c: ReadColumn.Strings =>
        }
      }
    }
  }
} finally ch.close()
```

### In-memory convenience (small inputs, tests)

`object Parquet` additionally offers array-based helpers that are **not** part of the
interface — `readFooter(bytes)`, `readFile(bytes)`, `assembleAuto(bytes)` (whole-file,
auto-unnesting a single single-level list column), `decodeColumnChunk(chunkBytes, cm)`,
and `writeFile(cols, numRows, codec): Array[Byte]` — for data known to fit in a 2 GB
array.

## Supported

Codecs `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `ZSTD` on read; page types `DATA_PAGE` (v1) and
`DICTIONARY_PAGE`; encodings `PLAIN`, `PLAIN_DICTIONARY`, `RLE_DICTIONARY`.
`DATA_PAGE_V2`, `DELTA_*` and `BYTE_STREAM_SPLIT` are not supported. The streaming path
covers flat columns; the nested single-list auto-unnest (`assembleAuto`) is an in-memory
convenience. The writer emits flat `UNCOMPRESSED` or `SNAPPY` PLAIN columns. The only
external dependency is `io.airlift:aircompressor` (Snappy/Zstd); `UNCOMPRESSED` and
`GZIP` use the JDK.

## Build, test, publish

```
sbt test      # typed round-trips, nulls, statistics, page-by-page streaming,
              # and an end-to-end read/write over a channel with no whole-file array
sbt publish   # publishes parquet-interface and parquet-core to GitHub Packages
```

The `release.yml` workflow publishes on a tag or a push to `main`/`master`, with
`GITHUB_ACTOR` and `GITHUB_TOKEN`.
