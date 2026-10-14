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
SOURCE_TABLE = "sales_order_lines"
TARGET = "bronze_oms_sales_order_lines"

# Source columns, landed as raw STRINGs
COLUMNS = [
    "sales_order_line_id",
    "sales_order_id",
    "line_number",
    "sku",
    "ship_from_warehouse_id",
    "qty_ordered",
    "qty_shipped",
    "qty_cancelled",
    "qty_backordered",
    "unit_price",
    "discount_pct",
    "net_line_amount",
    "line_status",
    "updated_at",
    "_batch_id",
    "_ingested_at",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw OMS sales_order_lines records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_oms_sales_order_lines_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: OMS REST API (template connector, see utilities/oms_api_source.py)
# ============================================================================

# @dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
# def bronze_oms_sales_order_lines_realtime():
#     return read_oms_api(SYSTEM, SOURCE_TABLE, COLUMNS)
