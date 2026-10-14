from pyspark import pipelines as dp

from utilities.bronze_sources import (
    REALTIME_SPARK_CONF,
    read_volume_csv,
    read_oms_api,
)

# ============================================================================
# Source configuration
# ============================================================================

SYSTEM = "oms"
SOURCE_TABLE = "returns"
TARGET = "bronze_oms_returns"

# Source columns, landed as raw STRINGs
COLUMNS = [
    "return_id",
    "sales_order_id",
    "sales_order_line_id",
    "fulfillment_id",
    "sku",
    "return_warehouse_id",
    "qty_returned",
    "return_reason",
    "rma_created_at",
    "received_date",
    "disposition",
    "restocking_fee_pct",
    "refund_amount",
    "refunded_date",
    "return_status",
    "updated_at",
    "_batch_id",
    "_ingested_at",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw OMS returns records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_oms_returns_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: OMS REST API (template connector, see utilities/oms_api_source.py)
# ============================================================================

# @dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
# def bronze_oms_returns_realtime():
#     return read_oms_api(SYSTEM, SOURCE_TABLE, COLUMNS)
