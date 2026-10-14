from pyspark import pipelines as dp

# ============================================================================
# Source configuration
# ============================================================================

VOLUME_PATH = "/Volumes/operations-catalog/supplychain/procurement"

FILE_PATTERN = "PNRao_SupplyChain_Supplier_Master.csv"

@dp.materialized_view(
    name="bronze_suppliers"
)
def bronze_suppliers():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_PATH}/{FILE_PATTERN}")
        .withColumnRenamed("unitCost", "unit_cost")
    )