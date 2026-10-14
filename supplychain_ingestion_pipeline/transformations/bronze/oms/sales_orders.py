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
SOURCE_TABLE = "sales_orders"
TARGET = "bronze_oms_sales_orders"

# Enable once the OMS API connector is implemented
REALTIME_ENABLED = False

# Source columns, landed as raw STRINGs
COLUMNS = [
    "sales_order_id",
    "customer_id",
    "order_ts",
    "order_channel",
    "customer_po_ref",
    "requested_delivery_date",
    "ship_to_city",
    "ship_to_state",
    "ship_to_postal_code",
    "currency",
    "payment_terms",
    "order_status",
    "cancel_reason",
    "cancelled_at",
    "line_count",
    "order_total",
    "created_at",
    "updated_at",
    "_batch_id",
    "_ingested_at",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw OMS sales_orders records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_oms_sales_orders_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: OMS REST API (template connector, see utilities/oms_api_source.py)
# ============================================================================

if REALTIME_ENABLED:

    @dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
    def bronze_oms_sales_orders_realtime():
        return read_oms_api(SYSTEM, SOURCE_TABLE, COLUMNS)
