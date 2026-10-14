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
SOURCE_TABLE = "customer_master"
TARGET = "bronze_erp_customer_master"

# Source columns, landed as raw STRINGs
COLUMNS = [
    "customer_id",
    "customer_name",
    "segment",
    "primary_channel",
    "city",
    "state",
    "postal_code",
    "country",
    "sales_region",
    "default_ship_from_warehouse_id",
    "payment_terms",
    "credit_limit",
    "allow_backorders",
    "customer_since",
    "status",
    "inactive_date",
    "_batch_id",
    "_ingested_at",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw ERP customer_master records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_erp_customer_master_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: new ERP extract files landing in S3 (Auto Loader)
# ============================================================================

# @dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
# def bronze_erp_customer_master_realtime():
#     return read_s3_csv(SYSTEM, SOURCE_TABLE, COLUMNS)
