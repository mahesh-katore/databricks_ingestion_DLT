
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

# -------------------
# CONFIG
# -------------------
storage_account = "databricksete12345678"
source_base_path = f"abfss://source@{storage_account}.dfs.core.windows.net"
v1 = "v1"

datasets = [
    {
        "source_name": "customers",
        "target_table": f"bronze_customers{v1}",
        "source_path": f"{source_base_path}/customers",
        "file_format": "parquet"
    },
    {
        "source_name": "orders",
        "target_table": f"bronze_orders{v1}",
        "source_path": f"{source_base_path}/orders",
        "file_format": "parquet"
    },
    {
        "source_name": "products",
        "target_table": f"bronze_products{v1}",
        "source_path": f"{source_base_path}/products",
        "file_format": "parquet"
    },
        {
        "source_name": "regions",
        "target_table": f"bronze_region{v1}",
        "source_path": f"{source_base_path}/regions",
        "file_format": "parquet"
    }
 
 
]

# -------------------
# CORE FUNCTION
# -------------------
def create_bronze_table(dataset):

    table_name = dataset["target_table"]

    @dp.table(
        name=table_name,
        comment=f"Bronze Delta table created from {dataset['source_name']}"
    )
    def bronze_table():
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", dataset["file_format"])
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(dataset["source_path"])
            .withColumn("ingestion_time", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
        )

# -------------------
# IMPORTANT: CALL FUNCTION
# -------------------
for dataset in datasets:
    create_bronze_table(dataset)
