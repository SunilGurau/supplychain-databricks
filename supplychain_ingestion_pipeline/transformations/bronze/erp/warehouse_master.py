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
SOURCE_TABLE = "warehouse_master"
TARGET = "bronze_erp_warehouse_master"

# Source columns, landed as raw STRINGs
COLUMNS = [
    "warehouse_id",
    "warehouse_name",
    "address_line1",
    "city",
    "state",
    "postal_code",
    "country",
    "latitude",
    "longitude",
    "warehouse_type",
    "size_sqft",
    "dock_doors",
    "opened_date",
    "timezone",
    "service_region",
    "status",
    "_batch_id",
    "_ingested_at",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw ERP warehouse_master records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_erp_warehouse_master_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: new ERP extract files landing in S3 (Auto Loader)
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
def bronze_erp_warehouse_master_realtime():
    return read_s3_csv(SYSTEM, SOURCE_TABLE, COLUMNS)
