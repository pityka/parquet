package parquet

final case class Column(
    name: String,
    ptype: Int,
    convertedType: Option[Int],
    optional: Boolean,
    numRows: Int,
    nonNull: Int,
    defLevels: Array[Int],
    plainValues: Array[Byte],
    minBytes: Option[Array[Byte]],
    maxBytes: Option[Array[Byte]]
)

final case class SchemaRaw(
    name: String,
    repetition: Int,
    ptype: Option[Int],
    convertedType: Int,
    numChildren: Int
)

final case class PColMeta(
    name: String,
    ptype: Int,
    convertedType: Int,
    codec: Int,
    numValues: Long,
    dataPageOffset: Long,
    dictPageOffset: Long,
    totalCompressed: Long,
    optional: Boolean,
    nullCount: Long,
    minBytes: Option[Array[Byte]],
    maxBytes: Option[Array[Byte]],
    path: Vector[String]
)

final case class Footer(
    rowGroups: Vector[Vector[PColMeta]],
    rowGroupRows: Vector[Int],
    schema: Vector[SchemaRaw]
) {
  def requireFlat(): Unit = rowGroups.foreach(_.foreach { cm =>
    if (cm.path.size > 1)
      throw new RuntimeException(
        s"cannot scan a nested/list column (leaf '${cm.name}') in place; use assembleAuto, which materializes and auto-unnests."
      )
  })
}
