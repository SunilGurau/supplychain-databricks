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
SOURCE_TABLE = "purchase_order_lines"
TARGET = "bronze_erp_purchase_order_lines"

# Source columns, landed as raw STRINGs
COLUMNS = [
    "po_line_id",
    "po_number",
    "line_number",
    "sku",
    "qty_ordered",
    "uom",
    "unit_cost",
    "line_amount",
    "qty_received",
    "qty_rejected",
    "promised_date",
    "last_receipt_date",
    "line_status",
    "updated_at",
    "_batch_id",
    "_ingested_at",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw ERP purchase_order_lines records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_erp_purchase_order_lines_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: new ERP extract files landing in S3 (Auto Loader)
# ============================================================================

# @dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
# def bronze_erp_purchase_order_lines_realtime():
#     return read_s3_csv(SYSTEM, SOURCE_TABLE, COLUMNS)
