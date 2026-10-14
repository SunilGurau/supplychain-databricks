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
SOURCE_TABLE = "shipments"
TARGET = "bronze_tms_shipments"
KAFKA_TOPIC = "tms_public_shipments"

# Source columns, landed as raw STRINGs
COLUMNS = [
    "shipmentId",
    "shipmentType",
    "referenceType",
    "referenceId",
    "carrierId",
    "serviceLevel",
    "mode",
    "trackingNumber",
    "originLocationId",
    "originCity",
    "originState",
    "originCountry",
    "destinationLocationId",
    "destinationCity",
    "destinationState",
    "destinationPostalCode",
    "destinationCountry",
    "shipDateTime",
    "plannedDeliveryDate",
    "actualDeliveryDateTime",
    "status",
    "delayReasonCode",
    "totalWeight",
    "weightUom",
    "handlingUnits",
    "distanceMiles",
    "freightCost",
    "currency",
    "createdAt",
    "updatedAt",
    "_batchId",
    "_ingestedAt",
]


dp.create_streaming_table(
    name=TARGET,
    comment="Raw TMS shipments records (historical extract + real-time feed)",
    table_properties={"quality": "bronze"},
)


# ============================================================================
# Historical refill: one-time load of the extract in the UC volume
# ============================================================================

@dp.append_flow(target=TARGET, name=f"{TARGET}_historical", once=True)
def bronze_tms_shipments_historical():
    return read_volume_csv(SYSTEM, SOURCE_TABLE, COLUMNS)


# ============================================================================
# Real-time: TMS events from Kafka
# ============================================================================

# @dp.append_flow(target=TARGET, name=f"{TARGET}_realtime", spark_conf=REALTIME_SPARK_CONF)
# def bronze_tms_shipments_realtime():
#     return read_kafka_json(SYSTEM, KAFKA_TOPIC, COLUMNS)
