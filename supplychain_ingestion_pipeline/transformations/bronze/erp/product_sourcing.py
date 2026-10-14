from pyspark import pipelines as dp

from utilities.bronze_sources import (
    REALTIME_SPARK_CONF,
    read_volume_csv,
    read_s3_csv,
)

# ============================================================================
# Source configuration
# ============================================================================

SYSTEM = "erp"
SOURCE_TABLE = "product_sourcing"
TARGET = "bronze_erp_product_sourcing"

# Source columns, landed as raw STRINGs
COLUMNS = [
    "sku",
    "supplier_id",
    "valid_from",
    "valid_to",
    "is_current",
    "supplier_lead_time_days",
    "moq_units",
    "_batch_id",
    "_ingested_at",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw ERP product_sourcing records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_erp_product_sourcing_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: new ERP extract files landing in S3 (Auto Loader)
# ============================================================================

# @dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
# def bronze_erp_product_sourcing_realtime():
#     return read_s3_csv(SYSTEM, SOURCE_TABLE, COLUMNS)
