"""
Shared source readers for the bronze layer.

Every bronze table is a streaming table fed by two append flows:

  * <table>_historical  (once=True)  -> CSV extract in the UC volume
  * <table>_realtime                 -> the system's live source
                                          erp: Auto Loader on S3
                                          wms: Debezium CDC events on Kafka
                                          tms: tracking events on Kafka
                                          oms: REST API (custom data source)

Both flows write into the same table, so every reader returns the same shape:
the source columns as raw STRINGs followed by the bronze metadata columns.

Pipeline configuration:
  volume       UC volume holding the extracts, e.g. /Volumes/operations-catalog/supplychain/landing
  erp_s3_path  S3 prefix with one folder per ERP table
  oms_api_url  OMS API base URL
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# No strict latency requirement: micro-batch every 5 seconds
REALTIME_SPARK_CONF = {"pipelines.trigger.interval": "5 seconds"}

# ============================================================================
# Kafka configuration
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = (
    "kafka-supplychain-supplychain-analytics.j.aivencloud.com:10157"
)

# PEM certificates stored in Unity Catalog Volume
KAFKA_CA_CERT = (
    "/Volumes/operations-catalog/supplychain/"
    "kafka-client-certificates/ca.pem"
)

KAFKA_CLIENT_CERT = (
    "/Volumes/operations-catalog/supplychain/"
    "kafka-client-certificates/service.cert"
)

KAFKA_CLIENT_KEY = (
    "/Volumes/operations-catalog/supplychain/"
    "kafka-client-certificates/service.key"
)


# ============================================================================
# Bronze metadata
# ============================================================================

SOURCE_STRUCT = StructType([
    StructField("type", StringType()),          # volume_csv | s3_csv | kafka | api
    StructField("uri", StringType()),           # file path / topic / API endpoint
    StructField("file_modification_time", TimestampType()),
    StructField("kafka_topic", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_timestamp", TimestampType()),
    StructField("kafka_key", StringType()),
])

CDC_STRUCT = StructType([
    StructField("op", StringType()),
    StructField("lsn", LongType()),
    StructField("source_ts_ms", LongType()),
    StructField("deleted", BooleanType()),
])


def _string_schema(columns):
    return StructType([StructField(c, StringType()) for c in columns])


def _source(source_type, **fields):
    fields["type"] = F.lit(source_type)

    return F.struct(*[
        fields.get(f.name, F.lit(None)).cast(f.dataType).alias(f.name)
        for f in SOURCE_STRUCT
    ])


def _bronze_select(
    df,
    business_columns,
    *,
    system,
    flow,
    source,
    cdc=None,
    raw_payload=None,
    rescued_data=None,
) -> DataFrame:

    def or_null(column, data_type):
        return (column if column is not None else F.lit(None)).cast(data_type)

    return df.select(
        *business_columns,
        or_null(rescued_data, StringType()).alias("_rescued_data"),
        or_null(raw_payload, StringType()).alias("_raw_payload"),
        source.alias("_source"),
        or_null(cdc, CDC_STRUCT).alias("_cdc"),
        F.lit(system).alias("_source_system"),
        F.lit(flow).alias("_ingest_flow"),
        F.current_timestamp().alias("_bronze_ingested_at"),
    )


# ============================================================================
# Historical refill: CSV extracts in the UC volume (batch, once)
# ============================================================================

def read_volume_csv(system, table, columns) -> DataFrame:
    spark = SparkSession.active()
    volume = spark.conf.get("volume")

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(_string_schema(columns))
        .load(f"{volume}/{system}/{table}.csv")
    )

    return _bronze_select(
        df,
        [F.col(c) for c in columns],
        system=system,
        flow="historical_backfill",
        source=_source(
            "volume_csv",
            uri=F.col("_metadata.file_path"),
            file_modification_time=F.col("_metadata.file_modification_time"),
        ),
        rescued_data=F.col("_rescued_data"),
    )


# ============================================================================
# Real-time: ERP extract files landing in S3 (Auto Loader)
# ============================================================================

def read_s3_csv(system, table, columns) -> DataFrame:
    spark = SparkSession.active()
    s3_path = spark.conf.get("erp_s3_path")

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(_string_schema(columns))
        .load(f"{s3_path}/{table}/")
    )

    return _bronze_select(
        df,
        [F.col(c) for c in columns],
        system=system,
        flow="realtime",
        source=_source(
            "s3_csv",
            uri=F.col("_metadata.file_path"),
            file_modification_time=F.col("_metadata.file_modification_time"),
        ),
        rescued_data=F.col("_rescued_data"),
    )


# ============================================================================
# Real-time: Kafka (mTLS with PEM certificates)
# ============================================================================

def read_kafka_json(system, topic, columns, cdc=False) -> DataFrame:
    """
    Extracts every source column as STRING from the JSON Kafka value.

    cdc=True reads the row from the Debezium envelope ($.payload) and also
    extracts __op, __lsn, __source_ts_ms and __deleted into the _cdc struct.
    """

    spark = SparkSession.active()

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)

        # Kafka PEM options expect the file CONTENT, not the file path
        .option("kafka.security.protocol", "SSL")
        .option("kafka.ssl.truststore.type", "PEM")
        .option("kafka.ssl.truststore.certificates", Path(KAFKA_CA_CERT).read_text())
        .option("kafka.ssl.keystore.type", "PEM")
        .option("kafka.ssl.keystore.certificate.chain", Path(KAFKA_CLIENT_CERT).read_text())
        .option("kafka.ssl.keystore.key", Path(KAFKA_CLIENT_KEY).read_text())
        .option("kafka.ssl.endpoint.identification.algorithm", "https")

        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()

        .withColumn("_value_string", F.col("value").cast("string"))
        .withColumn("_value", F.try_parse_json(F.col("_value_string")))
    )

    payload_path = "$.payload" if cdc else "$"

    def field(name, data_type="string"):
        return F.try_variant_get(
            F.col("_value"), f"{payload_path}['{name}']", data_type
        )

    cdc_column = None
    if cdc:
        cdc_column = F.struct(
            field("__op").alias("op"),
            field("__lsn", "bigint").alias("lsn"),
            field("__source_ts_ms", "bigint").alias("source_ts_ms"),
            field("__deleted", "boolean").alias("deleted"),
        )

    return _bronze_select(
        df,
        [field(c).alias(c) for c in columns],
        system=system,
        flow="realtime",
        source=_source(
            "kafka",
            uri=F.col("topic"),
            kafka_topic=F.col("topic"),
            kafka_partition=F.col("partition"),
            kafka_offset=F.col("offset"),
            kafka_timestamp=F.col("timestamp"),
            kafka_key=F.col("key").cast("string"),
        ),
        cdc=cdc_column,
        raw_payload=F.col("_value_string"),
    )


# ============================================================================
# Real-time: OMS REST API (custom Python data source - template)
# ============================================================================

def read_oms_api(system, table, columns) -> DataFrame:
    from utilities.oms_api_source import (
        OmsApiDataSource,
        RAW_PAYLOAD_COLUMN,
        REQUEST_URL_COLUMN,
    )

    spark = SparkSession.active()
    spark.dataSource.register(OmsApiDataSource)

    df = (
        spark.readStream
        .format(OmsApiDataSource.name())
        .option("base_url", spark.conf.get("oms_api_url"))
        .option("endpoint", table)
        .option("columns", ",".join(columns))
        .load()
    )

    return _bronze_select(
        df,
        [F.col(c) for c in columns],
        system=system,
        flow="realtime",
        source=_source("api", uri=F.col(REQUEST_URL_COLUMN)),
        raw_payload=F.col(RAW_PAYLOAD_COLUMN),
    )
