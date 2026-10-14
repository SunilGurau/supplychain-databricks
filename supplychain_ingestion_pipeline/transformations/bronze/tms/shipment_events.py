from pyspark import pipelines as dp

from utilities.bronze_sources import (
    REALTIME_SPARK_CONF,
    read_volume_csv,
    read_kafka_json,
)

# ============================================================================
# Source configuration
# ============================================================================

SYSTEM = "tms"
SOURCE_TABLE = "shipment_events"
TARGET = "bronze_tms_shipment_events"
KAFKA_TOPIC = "tms_public_shipment_events"

# Enable once the upstream team grants Kafka access
REALTIME_ENABLED = False

# Source columns, landed as raw STRINGs
COLUMNS = [
    "eventId",
    "shipmentId",
    "trackingNumber",
    "carrierId",
    "eventCode",
    "eventDateTime",
    "eventLocation",
    "exceptionReasonCode",
    "_batchId",
    "_ingestedAt",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw TMS shipment_events records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_tms_shipment_events_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: TMS events from Kafka
# ============================================================================

if REALTIME_ENABLED:

    @dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
    def bronze_tms_shipment_events_realtime():
        return read_kafka_json(SYSTEM, KAFKA_TOPIC, COLUMNS)
