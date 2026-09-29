from pyspark import pipelines as dp

# ============================================================================
# Source configuration
# ============================================================================

VOLUME_PATH = "/Volumes/operations-catalog/supplychain/procurement"

FILE_PATTERN = "PNRao_SupplyChain_Raw_Materials_Master.csv"


@dp.table(
    name="`operations-catalog`.supplychain.bronze_raw_materials",
    comment="Raw materials master data from procurement volume"
)
def bronze_raw_materials():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", FILE_PATTERN)
        .load(VOLUME_PATH)
    )