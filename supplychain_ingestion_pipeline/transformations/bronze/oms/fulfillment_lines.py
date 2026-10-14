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
SOURCE_TABLE = "fulfillment_lines"
TARGET = "bronze_oms_fulfillment_lines"

# Enable once the OMS API connector is implemented
REALTIME_ENABLED = False

# Source columns, landed as raw STRINGs
COLUMNS = [
    "fulfillment_line_id",
    "fulfillment_id",
    "sales_order_line_id",
    "sku",
    "qty_shipped",
    "created_at",
    "_batch_id",
    "_ingested_at",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw OMS fulfillment_lines records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_oms_fulfillment_lines_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: OMS REST API (template connector, see utilities/oms_api_source.py)
# ============================================================================

if REALTIME_ENABLED:

    @dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
    def bronze_oms_fulfillment_lines_realtime():
        return read_oms_api(SYSTEM, SOURCE_TABLE, COLUMNS)
