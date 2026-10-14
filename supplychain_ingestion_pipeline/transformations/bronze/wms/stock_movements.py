from pyspark import pipelines as dp

from utilities.bronze_sources import (
    REALTIME_SPARK_CONF,
    read_volume_csv,
    read_kafka_json,
)

# ============================================================================
# Source configuration
# ============================================================================

SYSTEM = "wms"
SOURCE_TABLE = "stock_movements"
TARGET = "bronze_wms_stock_movements"
KAFKA_TOPIC = "wms_public_stock_movements"

# Source columns, landed as raw STRINGs
COLUMNS = [
    "MOVEMENT_ID",
    "MOVEMENT_TS",
    "WAREHOUSE_ID",
    "SKU",
    "MOVEMENT_TYPE",
    "DIRECTION",
    "QUANTITY",
    "UOM",
    "REF_DOC_TYPE",
    "REF_DOC_ID",
    "REASON_CODE",
    "USER_ID",
    "_BATCH_ID",
    "_INGESTED_AT",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw WMS stock_movements records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_wms_stock_movements_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: WMS CDC events (Debezium) from Kafka
# ============================================================================

# @dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
# def bronze_wms_stock_movements_realtime():
#     return read_kafka_json(SYSTEM, KAFKA_TOPIC, COLUMNS, cdc=True)
