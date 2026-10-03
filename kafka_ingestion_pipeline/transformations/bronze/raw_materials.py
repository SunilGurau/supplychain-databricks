from pyspark import pipelines as dp
from pyspark.sql.types import *

# ============================================================================
# Source configuration
# ============================================================================

VOLUME_PATH = "/Volumes/operations-catalog/supplychain/procurement"

FILE_PATTERN = "PNRao_SupplyChain_Raw_Materials_Master.csv"


@dp.materialized_view(
    name="bronze_raw_materials"
)
def bronze_raw_materials():
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unitCost", DecimalType(10,2), True)
    ])
    return (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(f"{VOLUME_PATH}/{FILE_PATTERN}")
        .withColumnRenamed("unitCost", "unit_cost")
    )