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
SOURCE_TABLE = "product_master"
TARGET = "bronze_erp_product_master"

# Source columns, landed as raw STRINGs
COLUMNS = [
    "sku",
    "product_name",
    "brand",
    "category",
    "subcategory",
    "gtin",
    "unit_of_measure",
    "case_pack_qty",
    "unit_weight_kg",
    "primary_supplier_id",
    "standard_cost",
    "list_price",
    "velocity_class",
    "launch_date",
    "discontinued_date",
    "lifecycle_status",
    "stocked_warehouses",
    "_batch_id",
    "_ingested_at",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw ERP product_master records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_erp_product_master_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: new ERP extract files landing in S3 (Auto Loader)
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
def bronze_erp_product_master_realtime():
    return read_s3_csv(SYSTEM, SOURCE_TABLE, COLUMNS)
