from pyspark import pipelines as dp

KAFKA_BOOTSTRAP_SERVERS = (
    "kafka-supplychain-supplychain-analytics.j.aivencloud.com:10157"
)

KAFKA_TOPIC = "supplychain_public_purchase_order_lines"

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
    name="`operations-catalog`.supplychain.bronze_purchase_order_lines",
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