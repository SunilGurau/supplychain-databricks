from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# =========================================================
# 2. Parse and flatten Debezium CDC event
# =========================================================

@dp.temporary_view(name="flat_purchase_order_lines")
def flat_purchase_order_lines():

    from pyspark.sql.functions import parse_json

    # -----------------------------------------------------
    # Parse key and value
    # -----------------------------------------------------

    casted_purchase_order_lines = (
    spark.readStream
    .table("bronze_purchase_order_lines")

    .withColumn(
        "key",
        parse_json(
            expr("try_cast(key AS STRING)")
        )
    )

    .withColumn(
        "value",
        parse_json(
            expr("try_cast(value AS STRING)")
        )
    )
)

    # -----------------------------------------------------
    # Flatten CDC event
    # -----------------------------------------------------

    return casted_purchase_order_lines.select(
        # Primary key
        expr("key:payload:POLineID")
            .try_cast("string")
            .alias("po_line_id"),

        # Business columns
        expr("value:payload:PO_ID")
            .try_cast("string")
            .alias("po_id"),

        expr("value:payload:MaterialID")
            .try_cast("string")
            .alias("material_id"),

        expr("value:payload:Quantity")
            .try_cast("int")
            .alias("quantity"),

        expr("value:payload:UnitCost")
            .try_cast("decimal(10,2)")
            .alias("unit_cost"),

        # -------------------------------------------------
        # CDC metadata
        # -------------------------------------------------

        expr("value:payload:__op")
            .try_cast("string")
            .alias("__op"),

        expr("value:payload:__deleted")
            .try_cast("boolean")
            .alias("__deleted"),

        expr("value:payload:__lsn")
            .try_cast("long")
            .alias("__lsn"),

        expr("value:payload:__source_ts_ms")
            .try_cast("long")
            .alias("__source_ts_ms"),

        expr("value:payload:__table")
            .try_cast("string")
            .alias("__table"),

        # Kafka metadata
        col("timestamp").alias("__timestamp"),
        col("topic").alias("__topic"),
        col("partition").alias("__partition"),
        col("offset").alias("__offset"),
        col("timestampType").alias("__timestamp_type")
    )


# =========================================================
# 3. Create Silver target
# =========================================================

dp.create_streaming_table(
    "silver_purchase_order_lines"
)


# =========================================================
# 4. Apply CDC changes
# =========================================================

dp.create_auto_cdc_flow(
    target="silver_purchase_order_lines",

    source="flat_purchase_order_lines",

    # Primary key of purchase_order_lines
    keys=["po_line_id"],

    # PostgreSQL WAL ordering
    sequence_by=col("__lsn"),

    # Delete events
    apply_as_deletes=expr("__op = 'd'"),

    # Don't expose CDC/Kafka metadata in Silver
    except_column_list=[
        "__op",
        "__deleted",
        "__lsn",
        "__source_ts_ms",
        "__table",
        "__timestamp",
        "__offset",
        "__topic",
        "__partition",
        "__timestamp_type"
    ]
)