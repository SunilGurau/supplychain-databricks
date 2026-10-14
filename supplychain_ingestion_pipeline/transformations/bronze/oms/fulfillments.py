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
SOURCE_TABLE = "fulfillments"
TARGET = "bronze_oms_fulfillments"

# Enable once the OMS API connector is implemented
REALTIME_ENABLED = False

# Source columns, landed as raw STRINGs
COLUMNS = [
    "fulfillment_id",
    "sales_order_id",
    "warehouse_id",
    "shipment_id",
    "carrier_code",
    "tracking_number",
    "service_level",
    "fulfillment_status",
    "released_at",
    "picked_at",
    "packed_at",
    "shipped_at",
    "delivered_at",
    "line_count",
    "unit_count",
    "updated_at",
    "_batch_id",
    "_ingested_at",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw OMS fulfillments records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_oms_fulfillments_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: OMS REST API (template connector, see utilities/oms_api_source.py)
# ============================================================================

if REALTIME_ENABLED:

    @dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
    def bronze_oms_fulfillments_realtime():
        return read_oms_api(SYSTEM, SOURCE_TABLE, COLUMNS)
