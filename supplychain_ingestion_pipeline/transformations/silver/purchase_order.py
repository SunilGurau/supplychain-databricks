from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# =========================================================
# 1. Decode Kafka binary key/value
# =========================================================

@dp.view(name="decoded_purchase_order")
def decoded_purchase_order():

    return (
        spark.readStream
        .table("bronze_purchase_order")
        .select(
            col("key")
                .try_cast("string")
                .alias("key"),

            col("value")
                .try_cast("string")
                .alias("value"),

            col("timestamp").alias("kafka_timestamp"),
            col("offset").alias("kafka_offset")
        )
    )


# =========================================================
# 2. Parse and flatten Debezium CDC event
# =========================================================

@dp.temporary_view(name="flat_purchase_order")
def flat_purchase_order():

    from pyspark.sql.functions import from_json, parse_json
    from pyspark.sql.types import (
        StructType,
        StructField,
        ArrayType,
        StringType,
        BooleanType
    )

    # -----------------------------------------------------
    # Schema for Kafka key
    # -----------------------------------------------------

    key_schema = StructType([
        StructField(
            "schema",
            StructType([
                StructField("type", StringType()),
                StructField(
                    "fields",
                    ArrayType(
                        StructType([
                            StructField("type", StringType()),
                            StructField("optional", BooleanType()),
                            StructField("field", StringType())
                        ])
                    )
                ),
                StructField("optional", BooleanType()),
                StructField("name", StringType())
            ])
        ),

        StructField(
            "payload",
            StructType([
                StructField("PO_ID", StringType())
            ])
        )
    ])

    # -----------------------------------------------------
    # Parse key and value
    # -----------------------------------------------------

    casted_purchase_order = (
        spark.readStream
        .table("decoded_purchase_order")

        .withColumn(
            "key",
            from_json(
                col("key"),
                key_schema
            )
        )

        .withColumn(
            "value",
            parse_json(col("value"))
        )
    )

    # -----------------------------------------------------
    # Flatten CDC event
    # -----------------------------------------------------

    return casted_purchase_order.select(

        # Primary key
        col("key.payload.PO_ID")
            .alias("po_id"),

        # Business columns
        expr("value:payload:SupplierID")
            .try_cast("string")
            .alias("supplier_id"),

        expr("value:payload:OrderDate")
            .try_cast("date")
            .alias("order_date"),

        expr("value:payload:ExpectedDeliveryDate")
            .try_cast("date")
            .alias("expected_delivery_date"),

        expr("value:payload:Status")
            .try_cast("string")
            .alias("status"),

        expr("value:payload:TotalCost")
            .try_cast("decimal(10,2)")
            .alias("total_cost"),

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
        col("kafka_timestamp"),
        col("kafka_offset")
    )


# =========================================================
# 3. Create Silver target
# =========================================================

dp.create_streaming_table(
    "silver_purchase_order"
)


# =========================================================
# 4. Apply CDC changes
# =========================================================

dp.create_auto_cdc_flow(
    target="silver_purchase_order",

    source="flat_purchase_order",

    # Primary key of purchase_order
    keys=["po_id"],

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
        "kafka_timestamp",
        "kafka_offset"
    ]
)