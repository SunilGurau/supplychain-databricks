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
SOURCE_TABLE = "inventory_snapshot"
TARGET = "bronze_wms_inventory_snapshot"
KAFKA_TOPIC = "wms_public_inventory_snapshot"

# Source columns, landed as raw STRINGs
COLUMNS = [
    "SNAPSHOT_DATE",
    "WAREHOUSE_ID",
    "SKU",
    "QTY_ON_HAND",
    "QTY_QUARANTINE",
    "QTY_AVAILABLE",
    "LAST_RECEIPT_DATE",
    "LAST_CYCLE_COUNT_DATE",
    "_BATCH_ID",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw WMS inventory_snapshot records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_wms_inventory_snapshot_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: WMS CDC events (Debezium) from Kafka
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
def bronze_wms_inventory_snapshot_realtime():
    return read_kafka_json(SYSTEM, KAFKA_TOPIC, COLUMNS, cdc=True)
