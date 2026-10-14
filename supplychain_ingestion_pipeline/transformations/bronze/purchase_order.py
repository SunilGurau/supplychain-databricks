# from pyspark import pipelines as dp
# from pyspark.sql.functions import col

# # Kafka configuration
# KAFKA_BOOTSTRAP_SERVERS = "kafka-supplychain-supplychain-analytics.j.aivencloud.com:10157"
# KAFKA_TRUSTSTORE = "/Volumes/operations-catalog/supplychain/kafka-client-certificates/truststore.jks"
# KAFKA_KEYSTORE = "/Volumes/operations-catalog/supplychain/kafka-client-certificates/keystore.jks"
# KAFKA_TOPIC = "supplychain.public.purchase_order"

# @dp.table(
#     name="`operations-catalog`.supplychain.bronze_purchase_order",
#     comment="Raw purchase order events from Aiven Kafka"
# )
# def bronze_purchase_order():
#     return (
#         spark.readStream
#         .format("kafka")
#         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
#         .option("subscribe", KAFKA_TOPIC)
        
#         # Mutual TLS configuration
#         .option("kafka.security.protocol", "SSL")
#         .option("kafka.ssl.truststore.location", KAFKA_TRUSTSTORE)
#         .option("kafka.ssl.truststore.password", dbutils.secrets.get("kafka", "truststore-password"))
#         .option("kafka.ssl.keystore.location", KAFKA_KEYSTORE)
#         .option("kafka.ssl.keystore.password", dbutils.secrets.get("kafka", "keystore-password"))
#         .option("kafka.ssl.key.password", dbutils.secrets.get("kafka", "key-password"))
#         .option("kafka.ssl.endpoint.identification.algorithm", "https")
        
#         .option("startingOffsets", "earliest")
#         .option("failOnDataLoss", "false")
        
#         .load()
#     )
##################################################
from pyspark import pipelines as dp

# ============================================================================
# Kafka configuration
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = (
    "kafka-supplychain-supplychain-analytics.j.aivencloud.com:10157"
)

KAFKA_TOPIC = "supplychain_public_purchase_order"

# PEM certificates stored in Unity Catalog Volume
KAFKA_CA_CERT = (
    "/Volumes/operations-catalog/supplychain/"
    "kafka-client-certificates/ca.pem"
)

KAFKA_CLIENT_CERT = (
    "/Volumes/operations-catalog/supplychain/"
    "kafka-client-certificates/service.cert"
)

KAFKA_CLIENT_KEY = (
    "/Volumes/operations-catalog/supplychain/"
    "kafka-client-certificates/service.key"
)


@dp.table(
    name="`operations-catalog`.supplychain.bronze_purchase_order",
    comment="Raw purchase order events from Aiven Kafka"
)
def bronze_purchase_order():

    print("========================================")
    print("Starting Kafka connection")
    print(f"Bootstrap server: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print("Using PEM client certificates")
    print("========================================")

    # Kafka PEM options expect the file CONTENT, not the file path
    ca_pem = dbutils.fs.head(KAFKA_CA_CERT)
    client_cert_pem = dbutils.fs.head(KAFKA_CLIENT_CERT)
    client_key_pem = dbutils.fs.head(KAFKA_CLIENT_KEY)

    df = (
        spark.readStream
        .format("kafka")

        # Kafka broker
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )

        # Topic
        .option(
            "subscribe",
            KAFKA_TOPIC
        )

        # --------------------------------------------------------------------
        # SSL / mTLS
        # --------------------------------------------------------------------

        .option(
            "kafka.security.protocol",
            "SSL"
        )

        # CA certificate used to verify the Kafka broker
        .option(
            "kafka.ssl.truststore.type",
            "PEM"
        )
        .option(
            "kafka.ssl.truststore.certificates",
            ca_pem
        )

        # Client certificate used to authenticate to Kafka
        .option(
            "kafka.ssl.keystore.type",
            "PEM"
        )
        .option(
            "kafka.ssl.keystore.certificate.chain",
            client_cert_pem
        )

        # Client private key
        .option(
            "kafka.ssl.keystore.key",
            client_key_pem
        )

        # Verify Kafka broker hostname against certificate
        .option(
            "kafka.ssl.endpoint.identification.algorithm",
            "https"
        )

        # --------------------------------------------------------------------
        # Streaming behavior
        # --------------------------------------------------------------------

        .option(
            "startingOffsets",
            "earliest"
        )

        .option(
            "failOnDataLoss",
            "false"
        )

        .load()
    )

    print("Kafka streaming DataFrame created successfully.")
    print("mTLS configuration has been applied.")

    return df